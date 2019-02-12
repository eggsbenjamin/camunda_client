package client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/eggsbenjamin/camunda_client/pkg/models"
	"github.com/pkg/errors"
)

const (
	InputVariableTypeString = "String"
)

type ResponseErrorBody struct {
	Type    string `json:"type,omitempty"`
	Message string `json:"message,omitempty"`
}

type StartProcessInstanceInput struct {
	ProcessID   string                     `json:"-,omitempty"`
	BusinessKey string                     `json:"businessKey,omitempty"`
	Variables   map[string]models.Variable `json:"variables,omitempty"`
}

type StartProcessInstanceOutput struct {
}

type FetchAndLockExternalTasksInput struct {
	WorkerID             string         `json:"workerId,omitempty"`
	MaxTasks             int64          `json:"maxTasks,omitempty"`
	UsePriority          bool           `json:"usePriority,omitempty"`
	AsyncResponseTimeout time.Duration  `json:"asyncResponseTimeout,omitempty"`
	Topics               []models.Topic `json:"topics,omitempty"`
}

type FetchAndLockExternalTasksOutput struct {
	Results []FetchAndLockExternalTasksOutputResult
}

type FetchAndLockExternalTasksOutputResult struct {
	ID           string `json:"id,omitempty"`
	ActivityID   string `json:"activityId,omitempty"`
	ExecutionID  string `json:"executionId,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	ErrorDetails string `json:"errorDetails,omitempty"`
	TopicName    string `json:"topicName,omitempty"`
	//LockExpirationTime time.Time                     `json:"lockExpirationTime,omitempty"`
	Retries   int64                      `json:"retries,omitempty"`
	Variables map[string]models.Variable `json:"variables,omitempty"`
}

type CompleteExternalTaskInput struct {
	WorkerID       string                     `json:"workerId,omitempty"`
	ExternalTaskID string                     `json:"-"`
	Variables      map[string]models.Variable `json:"variables,omitempty"`
}

type FailExternalTaskInput struct {
	WorkerID       string        `json:"workerId,omitempty"`
	ExternalTaskID string        `json:"-"`
	ErrorMessage   string        `json:"errorMessage,omitempty"`
	ErrorDetails   string        `json:"errorDetails,omitempty"`
	Retries        int64         `json:"retries,omitempty"`
	RetryTimeout   time.Duration `json:"retryTimeout,omitempty"`
}

type Client interface {
	StartProcessInstance(StartProcessInstanceInput) (StartProcessInstanceOutput, error)
	FetchAndLockExternalTasks(FetchAndLockExternalTasksInput) (FetchAndLockExternalTasksOutput, error)
	CompleteExternalTask(CompleteExternalTaskInput) error
	FailExternalTask(FailExternalTaskInput) error
}

type ClientConfig struct {
	BrokerURL  string
	HTTPClient *http.Client
}

type client struct {
	cfg ClientConfig
}

func New(cfg ClientConfig) Client {
	return &client{
		cfg: cfg,
	}
}

var (
	ErrInvalidInput    = errors.New("invalid input")
	ErrProcessNotFound = errors.New("process not found")
	ErrUnexpected      = errors.New("unexpected error")
)

func (c *client) StartProcessInstance(input StartProcessInstanceInput) (StartProcessInstanceOutput, error) {
	var (
		buf    bytes.Buffer
		output StartProcessInstanceOutput
	)
	if err := json.NewEncoder(&buf).Encode(input); err != nil {
		return output, err
	}

	resp, err := http.Post(c.cfg.BrokerURL+"/engine-rest/process-definition/key/"+input.ProcessID+"/start", "application/json", &buf)
	if err != nil {
		return output, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var respBody ResponseErrorBody
		json.NewDecoder(resp.Body).Decode(&respBody) // don't handle error here because it's an implementation, better to return a meaningful error to the user (log maybe?)

		switch resp.StatusCode {
		case http.StatusBadRequest:
			return output, errors.Wrap(ErrInvalidInput, respBody.Message)
		case http.StatusNotFound:
			return output, errors.Wrap(ErrProcessNotFound, respBody.Message)
		default:
			return output, errors.Wrap(ErrUnexpected, respBody.Message)
		}
	}

	return output, json.NewDecoder(resp.Body).Decode(&output)
}

func (c *client) FetchAndLockExternalTasks(input FetchAndLockExternalTasksInput) (FetchAndLockExternalTasksOutput, error) {
	var (
		buf    bytes.Buffer
		output FetchAndLockExternalTasksOutput
	)
	if err := json.NewEncoder(&buf).Encode(input); err != nil {
		return output, err
	}

	resp, err := http.Post(c.cfg.BrokerURL+"/engine-rest/external-task/fetchAndLock", "application/json", &buf)
	if err != nil {
		return output, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var respBody ResponseErrorBody
		json.NewDecoder(resp.Body).Decode(&respBody) // don't handle error here because it's an implementation, better to return a meaningful error to the user (log maybe?)

		return output, errors.Wrap(ErrUnexpected, respBody.Message)
	}

	return output, json.NewDecoder(resp.Body).Decode(&output.Results)
}

func (c *client) CompleteExternalTask(input CompleteExternalTaskInput) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(input); err != nil {
		return err
	}

	resp, err := http.Post(c.cfg.BrokerURL+"/engine-rest/external-task/"+input.ExternalTaskID+"/complete", "application/json", &buf)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		var respBody ResponseErrorBody
		json.NewDecoder(resp.Body).Decode(&respBody) // don't handle error here because it's an implementation, better to return a meaningful error to the user (log maybe?)

		switch resp.StatusCode {
		case http.StatusBadRequest:
			return errors.Wrap(ErrInvalidInput, respBody.Message)
		case http.StatusNotFound:
			return errors.Wrap(ErrProcessNotFound, respBody.Message)
		default:
			return errors.Wrap(ErrUnexpected, respBody.Message)
		}
	}

	return nil
}

func (c *client) FailExternalTask(input FailExternalTaskInput) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(input); err != nil {
		return err
	}

	resp, err := http.Post(c.cfg.BrokerURL+"/engine-rest/external-task/"+input.ExternalTaskID+"/failure", "application/json", &buf)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		var respBody ResponseErrorBody
		json.NewDecoder(resp.Body).Decode(&respBody) // don't handle error here because it's an implementation, better to return a meaningful error to the user (log maybe?)

		switch resp.StatusCode {
		case http.StatusBadRequest:
			return errors.Wrap(ErrInvalidInput, respBody.Message)
		case http.StatusNotFound:
			return errors.Wrap(ErrProcessNotFound, respBody.Message)
		default:
			return errors.Wrap(ErrUnexpected, respBody.Message)
		}
	}

	return nil
}
