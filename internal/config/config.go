package config

import (
	"errors"
	"os"
)

type Config struct {
	KafkaBroker            string
	KafkaTopic             string
	KafkaEventsTopic       string
	KafkaDlqTopic          string
	KafkaProjectionGroupID string
	KafkaGroupID           string
	HTTPAddr               string
	DatabaseURL            string
}

func Load() (*Config, error) {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	kafkaEventsTopic := os.Getenv("KAFKA_EVENTS_TOPIC")
	kafkaDlqTopic := os.Getenv("KAFKA_DLQ_TOPIC")
	kafkaProjectionGroupID := os.Getenv("KAFKA_PROJECTION_GROUP_ID")
	kafkaGroupID := os.Getenv("KAFKA_GROUP_ID")
	httpAddr := os.Getenv("HTTP_ADDR")
	databaseURL := os.Getenv("DATABASE_URL")

	if kafkaBroker == "" {
		return nil, errors.New("KAFKA_BROKER is required")
	}

	if kafkaTopic == "" {
		return nil, errors.New("KAFKA_TOPIC is required")
	}

	if kafkaEventsTopic == "" {
		return nil, errors.New("KAFKA_EVENTS_TOPIC is required")
	}

	if kafkaDlqTopic == "" {
		return nil, errors.New("KAFKA_DLQ_TOPIC is required")
	}

	if kafkaProjectionGroupID == "" {
		return nil, errors.New("KAFKA_PROJECTION_GROUP_ID is required")
	}

	if kafkaGroupID == "" {
		return nil, errors.New("KAFKA_GROUP_ID is required")
	}

	if httpAddr == "" {
		return nil, errors.New("HTTP_ADDR is required")
	}

	if databaseURL == "" {
		return nil, errors.New("DATABASE_URL is required")
	}

	return &Config{
		KafkaBroker:            kafkaBroker,
		KafkaTopic:             kafkaTopic,
		KafkaEventsTopic:       kafkaEventsTopic,
		KafkaDlqTopic:          kafkaDlqTopic,
		KafkaProjectionGroupID: kafkaProjectionGroupID,
		KafkaGroupID:           kafkaGroupID,
		HTTPAddr:               httpAddr,
		DatabaseURL:            databaseURL,
	}, nil
}
