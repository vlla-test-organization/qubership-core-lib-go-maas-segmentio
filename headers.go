package segmentio

import (
	kafkago "github.com/segmentio/kafka-go"
)

func ExtractHeaders(headers []kafkago.Header) map[string]interface{} {
	result := map[string]interface{}{}
	for _, h := range headers {
		result[h.Key] = string(h.Value)
	}
	return result
}

func BuildHeaders(ctxData map[string]string) []kafkago.Header {
	var result []kafkago.Header
	for k, v := range ctxData {
		result = append(result, kafkago.Header{Key: k, Value: []byte(v)})
	}
	return result
}
