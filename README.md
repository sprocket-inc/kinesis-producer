# Amazon kinesis producer [![Build status][travis-image]][travis-url] [![License][license-image]][license-url] [![GoDoc][godoc-img]][godoc-url]
> A KPL-like batch producer for Amazon Kinesis built on top of the official Go AWS SDK  
and using the same aggregation format that [KPL][kpl-url] use.  

### Useful links
- [Documentation][godoc-url]
- [Aggregation format][aggregation-format-url]
- [Considerations When Using KPL Aggregation][kpl-aggregation]
- [Consumer De-aggregation][de-aggregation]

### Example
```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/a8m/kinesis-producer"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func main() {
	// Load AWS SDK v2 configuration
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	client := kinesis.NewFromConfig(cfg)

	pr, err := producer.New(&producer.Config{
		StreamName:   "test",
		BacklogCount: 2000,
		Client:       client,
		// RateLimit:  150,  // Optional: default is 150 (150% of shard limits)
		// ShardMapRefreshTimeout: 30 * time.Second,  // Optional: default is 30s
	})
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}

	if err := pr.Start(); err != nil {
		log.Fatalf("failed to start producer: %v", err)
	}

	// Handle failures
	go func() {
		for r := range pr.NotifyFailures() {
			// r contains `Data`, `PartitionKey` and `Error()`
			log.Printf("failed to put record: %v", r)
		}
	}()

	go func() {
		for i := 0; i < 5000; i++ {
			err := pr.Put([]byte("foo"), "bar")
			if err != nil {
				log.Fatalf("error producing: %v", err)
			}
		}
	}()

	time.Sleep(3 * time.Second)
	pr.Stop()
}
```

#### Specifying logger implementation
`producer.Config` takes an optional `logging.Logger` implementation.

##### Using a custom logger
```go
customLogger := &CustomLogger{}

&producer.Config{
  StreamName:   "test",
  BacklogCount: 2000,
  Client:       client,
  Logger:       customLogger,
}
```

#### Using logrus

```go
import (
	"github.com/sirupsen/logrus"
	producer "github.com/a8m/kinesis-producer"
	"github.com/a8m/kinesis-producer/loggers"
)

log := logrus.New()

&producer.Config{
  StreamName:   "test",
  BacklogCount: 2000,
  Client:       client,
  Logger:       loggers.Logrus(log),
}
```

kinesis-producer ships with three logger implementations.

- `producer.Standard` used the standard library logger
- `loggers.Logrus` uses logrus logger
- `loggers.Zap` uses zap logger

### License
MIT

[godoc-url]: https://godoc.org/github.com/a8m/kinesis-producer
[godoc-img]: https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square
[kpl-url]: https://github.com/awslabs/amazon-kinesis-producer
[de-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-kpl-consumer-deaggregation.html
[kpl-aggregation]: http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-producer-adv-aggregation.html
[aggregation-format-url]: https://github.com/a8m/kinesis-producer/blob/master/aggregation-format.md
[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square
[license-url]: LICENSE
[travis-image]: https://img.shields.io/travis/a8m/kinesis-producer.svg?style=flat-square
[travis-url]: https://travis-ci.org/a8m/kinesis-producer

