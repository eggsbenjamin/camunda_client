package main

import (
	"log"
	"net/http"

	"github.com/eggsbenjamin/camunda_client/pkg/client"
	"github.com/eggsbenjamin/camunda_client/pkg/env"
)

func main() {
	brokerClient := client.New(
		client.ClientConfig{
			BrokerURL:  env.MustGetEnv("BROKER_URL"),
			HTTPClient: http.DefaultClient,
		},
	)

	log.Println("starting process...")
	_, err := brokerClient.StartProcessInstance(
		client.StartProcessInstanceInput{
			ProcessID:   env.MustGetEnv("PROCESS_ID"),
			BusinessKey: "12345",
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
