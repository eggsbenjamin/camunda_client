package worker

import (
	"sync"
	"time"

	"github.com/eggsbenjamin/camunda_client/pkg/client"
	"github.com/eggsbenjamin/camunda_client/pkg/models"
	"github.com/pkg/errors"
)

type Task struct {
	client client.Client
	TaskDefinition
}

type TaskDefinition struct {
	client client.Client

	ID           string `json:"id,omitempty"`
	ActivityID   string `json:"activityId,omitempty"`
	ExecutionID  string `json:"executionId,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	ErrorDetails string `json:"errorDetails,omitempty"`
	TopicName    string `json:"topicName,omitempty"`
	WorkerID     string `json:"workerId,omitempty"`
	//LockExpirationTime time.Time                     `json:"lockExpirationTime,omitempty"`
	Retries   int64                      `json:"retries,omitempty"`
	Variables map[string]models.Variable `json:"variables,omitempty"`
}

func NewTask(client client.Client, definition TaskDefinition) Task {
	return Task{
		client,
		definition,
	}
}

func (t *Task) Complete() error {
	return t.client.CompleteExternalTask(
		client.CompleteExternalTaskInput{
			ExternalTaskID: t.ID,
			Variables:      t.Variables,
			WorkerID:       t.WorkerID,
		},
	)
}

func (t *Task) Fail() error {
	return t.client.FailExternalTask(
		client.FailExternalTaskInput{
			ExternalTaskID: t.ID,
			WorkerID:       t.WorkerID,
		},
	)
}

type TaskHandlerFunc func(Task)

type WorkerPoolConfig struct {
	MaxTasks     int64
	WorkerID     string
	UsePriority  bool
	BrokerClient client.Client
	PollInterval time.Duration
}

type WorkerPool struct {
	cfg          WorkerPoolConfig
	mu           sync.Mutex
	taskHandlers map[string]struct {
		topic   models.Topic
		handler TaskHandlerFunc
	}
}

func NewWorkerPool(cfg WorkerPoolConfig) WorkerPool {
	return WorkerPool{
		cfg: cfg,
		taskHandlers: make(
			map[string]struct {
				topic   models.Topic
				handler TaskHandlerFunc
			},
		),
	}
}

func (w *WorkerPool) RegisterExternalTaskHandler(topic models.Topic, handler TaskHandlerFunc) error {
	defer w.mu.Unlock()
	w.mu.Lock()

	if _, ok := w.taskHandlers[topic.TopicName]; ok {
		return errors.Errorf("existing handler for topic: %s", topic.TopicName)
	}

	w.taskHandlers[topic.TopicName] = struct {
		topic   models.Topic
		handler TaskHandlerFunc
	}{
		topic:   topic,
		handler: handler,
	}

	return nil
}

func (w *WorkerPool) Listen() error {
	for {
		if err := w.poll(); err != nil {
			return err
		}

		time.Sleep(w.cfg.PollInterval)
	}
}

func (w *WorkerPool) poll() error {
	fetchAndLockExternalTasksInput := client.FetchAndLockExternalTasksInput{
		WorkerID:    w.cfg.WorkerID,
		MaxTasks:    w.cfg.MaxTasks,
		UsePriority: w.cfg.UsePriority,
	}

	for _, taskHandler := range w.taskHandlers {
		fetchAndLockExternalTasksInput.Topics = append(fetchAndLockExternalTasksInput.Topics, taskHandler.topic)
	}

	lockedExternalTasks, err := w.cfg.BrokerClient.FetchAndLockExternalTasks(fetchAndLockExternalTasksInput)
	if err != nil {
		return err
	}

	for _, externalTask := range lockedExternalTasks.Results {
		w.taskHandlers[externalTask.TopicName].handler(
			NewTask(
				w.cfg.BrokerClient,
				TaskDefinition{
					WorkerID:     w.cfg.WorkerID,
					ID:           externalTask.ID,
					ActivityID:   externalTask.ActivityID,
					ErrorDetails: externalTask.ErrorDetails,
					ErrorMessage: externalTask.ErrorMessage,
					ExecutionID:  externalTask.ExecutionID,
					Retries:      externalTask.Retries,
					TopicName:    externalTask.TopicName,
					Variables:    externalTask.Variables,
				},
			),
		)
	}

	return nil
}
