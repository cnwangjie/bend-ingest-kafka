package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	bend_ingest_kafka "github.com/cnwangjie/bend-ingest-kafka"
	"github.com/cnwangjie/bend-ingest-kafka/config"
)

var (
	kafkaBrokers       = []string{"localhost:9092"}
	kafkaTopic         = "test"
	kafkaConsumerGroup = "test-group"
	batchSize          = 1000
	batchMaxInterval   = 5 * time.Second
	dataFormat         = "json"
	databendDSN        = "root:@tcp(127.0.0.1:3306)/test"
	databendTable      = "default.test"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigch := make(chan os.Signal, 1)
		signal.Notify(sigch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM)
		<-sigch
		cancel()
	}()

	cfg := parseConfig()
	ig := bend_ingest_kafka.NewDatabendIngester(cfg)
	if !cfg.IsJsonTransform {
		err := ig.CreateRawTargetTable()
		if err != nil {
			panic(err)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		w := bend_ingest_kafka.NewConsumeWorker(cfg, fmt.Sprintf("worker-%d", i), ig)
		go func() {
			w.Run(ctx)
			wg.Done()
		}()
	}
	wg.Wait()
}

func parseConfigWithFile() *config.Config {
	cfg, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}
	return cfg
}

func parseConfig() *config.Config {
	// if config/conf.json exists, use it
	if _, err := os.Stat("config/conf.json"); err == nil {
		return parseConfigWithFile()
	}

	cfg := config.Config{}
	flag.StringVar(&cfg.KafkaBootstrapServers, "kafka-bootstrap-servers", "127.0.0.1:64103", "Kafka bootstrap servers")
	flag.StringVar(&cfg.KafkaTopic, "kafka-topic", "test", "Kafka topic")
	flag.StringVar(&cfg.KafkaConsumerGroup, "kafka-consumer-group", "kafka-bend-ingest", "Kafkaconsumer group")
	flag.StringVar(&cfg.DatabendDSN, "databend-dsn", "http://root:root@localhost:8002", "Databend DSN")
	flag.StringVar(&cfg.DatabendTable, "databend-table", "test_ingest", "Databend table")
	flag.StringVar(&cfg.MockData, "mock-data", "", "generate mock data to databend")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1024, "Batch size")
	flag.IntVar(&cfg.Workers, "workers", 1, "Number of workers")
	flag.IntVar(&cfg.BatchMaxInterval, "batch-max-interval", 30, "Batch max interval")
	flag.StringVar(&cfg.DataFormat, "data-format", "json", "kafka data format")
	flag.BoolVar(&cfg.CopyPurge, "copy-purge", false, "purge data before copy")
	flag.BoolVar(&cfg.CopyForce, "copy-force", false, "force copy data")
	flag.BoolVar(&cfg.IsJsonTransform, "is-json-transform", true, "transform json data")

	flag.Parse()
	return &cfg
}