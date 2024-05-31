package bend_ingest_kafka

import "strings"

func parseKafkaServers(kafkaServerStr string) []string {
	kafkaServers := strings.Split(kafkaServerStr, ",")
	if len(kafkaServers) == 0 {
		panic("should have kafka servers")
	}
	return kafkaServers
}
