package producer

import (
	"context"
	"errors"
	"log"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type responseMock struct {
	Response *k.PutRecordsOutput
	Error    error
}

type clientMock struct {
	calls     int
	responses []responseMock
	incoming  map[int][]string
}

func (c *clientMock) PutRecords(_ context.Context, input *k.PutRecordsInput, _ ...func(*k.Options)) (*k.PutRecordsOutput, error) {
	res := c.responses[c.calls]
	for _, r := range input.Records {
		c.incoming[c.calls] = append(c.incoming[c.calls], *r.PartitionKey)
	}
	c.calls++
	if res.Error != nil {
		return nil, res.Error
	}
	return res.Response, nil
}

type testCase struct {
	// configuration
	name    string      // test name
	config  *Config     // test config
	records []string    // all outgoing records(partition keys and data too)
	putter  *clientMock // mocked client

	// expectations
	outgoing map[int][]string // [call number][partition keys]
}

// createMockShardMap creates a simple mock ShardMap for testing
func createMockShardMap() *ShardMap {
	// Create a simple ShardMap with 2 shards
	maxHash := new(big.Int)
	maxHash.SetString("340282366920938463463374607431768211455", 10) // 2^128 - 1

	midPoint := new(big.Int).Div(maxHash, big.NewInt(2))

	return &ShardMap{
		shards: []ShardRange{
			{
				ShardID:         "shard-000001",
				StartingHashKey: big.NewInt(0),
				EndingHashKey:   midPoint,
			},
			{
				ShardID:         "shard-000002",
				StartingHashKey: new(big.Int).Add(midPoint, big.NewInt(1)),
				EndingHashKey:   maxHash,
			},
		},
		state:     ShardMapStateReady, // Mark as ready to prevent refresh attempts
		updatedAt: time.Now(),
		logger:    &StdLogger{log.New(os.Stdout, "", log.LstdFlags)},
	}
}

func genBulk(n int, s string) (ret []string) {
	for i := 0; i < n; i++ {
		ret = append(ret, s)
	}
	return
}

var testCases = []testCase{
	{
		"one record with batch count 1",
		&Config{BatchCount: 1, ShardMap: createMockShardMap()},
		[]string{"hello"},
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int32(0),
					},
				},
			}},
		map[int][]string{
			0: []string{"hello"},
		},
	},
	{
		"two records with batch count 1",
		&Config{BatchCount: 1, AggregateBatchCount: 1, ShardMap: createMockShardMap()},
		[]string{"hello", "world"},
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int32(0),
					},
				},
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int32(0),
					},
				},
			}},
		map[int][]string{
			0: []string{"hello"},
			1: []string{"world"},
		},
	},
	{
		"two records with batch count 2, simulating retries",
		&Config{BatchCount: 2, AggregateBatchCount: 1, ShardMap: createMockShardMap()},
		[]string{"hello", "world"},
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int32(1),
						Records: []ktypes.PutRecordsResultEntry{
							{SequenceNumber: aws.String("3"), ShardId: aws.String("1")},
							{ErrorCode: aws.String("400")},
						},
					},
				},
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int32(0),
					},
				},
			}},
		map[int][]string{
			0: []string{"hello", "world"},
			1: []string{"world"},
		},
	},
	{
		"2 bulks of 10 records",
		&Config{BatchCount: 10, AggregateBatchCount: 1, BacklogCount: 1, ShardMap: createMockShardMap()},
		genBulk(20, "foo"),
		&clientMock{
			incoming: make(map[int][]string),
			responses: []responseMock{
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int32(0),
					},
				},
				{
					Error: nil,
					Response: &k.PutRecordsOutput{
						FailedRecordCount: aws.Int32(0),
					},
				},
			}},
		// Aggregated records use placeholder partition key "a" and explicit hash key for routing
		map[int][]string{
			0: genBulk(10, "a"),
			1: genBulk(10, "a"),
		},
	},
}

func TestProducer(t *testing.T) {
	for _, test := range testCases {
		test.config.StreamName = test.name
		test.config.MaxConnections = 1
		test.config.Client = test.putter
		p, err := New(test.config)
		if err != nil {
			t.Fatalf("failed to create producer: %v", err)
		}
		if err := p.Start(); err != nil {
			t.Fatalf("failed to start producer: %v", err)
		}
		var wg sync.WaitGroup
		wg.Add(len(test.records))
		for _, r := range test.records {
			go func(s string) {
				p.Put([]byte(s), s)
				wg.Done()
			}(r)
		}
		wg.Wait()
		p.Stop()
		for k, v := range test.putter.incoming {
			if len(v) != len(test.outgoing[k]) {
				t.Errorf("failed test: %s\n\texcpeted:%v\n\tactual:  %v", test.name,
					test.outgoing, test.putter.incoming)
			}
		}
	}
}

func TestNotify(t *testing.T) {
	kError := errors.New("ResourceNotFoundException: Stream foo under account X not found")
	p, err := New(&Config{
		StreamName:          "foo",
		MaxConnections:      1,
		BatchCount:          1,
		AggregateBatchCount: 10,
		ShardMap:            createMockShardMap(),
		Client: &clientMock{
			incoming:  make(map[int][]string),
			responses: []responseMock{{Error: kError}},
		},
	})
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("failed to start producer: %v", err)
	}
	records := genBulk(10, "bar")
	var wg sync.WaitGroup
	wg.Add(len(records))
	failed := 0
	done := make(chan bool, 1)
	go func() {
		for _ = range p.NotifyFailures() {
			failed++
			wg.Done()
		}
		// expect producer close the failures channel
		done <- true
	}()
	for _, r := range records {
		p.Put([]byte(r), r)
	}
	wg.Wait()
	p.Stop()

	if failed != len(records) {
		t.Errorf("failed test: NotifyFailure\n\texcpeted:%v\n\tactual:%v", failed, len(records))
	}

	if !<-done {
		t.Error("failed test: NotifyFailure\n\texpect failures channel to be closed")
	}
}
