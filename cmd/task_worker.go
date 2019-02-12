package main

import (
	"log"
	"net/http"
	"time"

	"github.com/eggsbenjamin/camunda_client/pkg/client"
	"github.com/eggsbenjamin/camunda_client/pkg/env"
	"github.com/eggsbenjamin/camunda_client/pkg/models"
	"github.com/eggsbenjamin/camunda_client/pkg/worker"
)

func main() {
	brokerClient := client.New(
		client.ClientConfig{
			BrokerURL:  env.MustGetEnv("BROKER_URL"),
			HTTPClient: http.DefaultClient,
		},
	)

	workerPool := worker.NewWorkerPool(
		worker.WorkerPoolConfig{
			WorkerID:     "test",
			BrokerClient: brokerClient,
			MaxTasks:     1,
			PollInterval: 10 * time.Second / time.Millisecond,
		},
	)

	workerPool.RegisterExternalTaskHandler(
		models.Topic{
			LockDuration: 5 * time.Second / time.Millisecond,
			TopicName:    "build",
		},
		func(task worker.Task) {
			log.Println("processing task", task.ID, "of type", task.ActivityID)

			if err := task.Complete(); err != nil {
				log.Println("unable to complete task", task.ID, "error:", err.Error())
			}

			log.Println("completed task", task.ID)
		},
	)

	log.Fatal(workerPool.Listen())
}
