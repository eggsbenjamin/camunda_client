package models

import "time"

type Variable struct {
	Value interface{} `json:"value,omitempty"`
	Type  string      `json:"type,omitempty"`
}

type Topic struct {
	TopicName            string        `json:"topicName,omitempty"`
	LockDuration         time.Duration `json:"lockDuration,omitempty"`
	ProcessDefinitionID  string        `json:"processDefinitionId,omitempty"`
	ProcessDefinitionKey string        `json:"processDefinitionKey,omitempty"`
}
