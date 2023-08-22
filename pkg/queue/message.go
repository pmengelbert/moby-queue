package queue

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/Azure/moby-packaging/pkg/archive"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	defaultAccountName = "moby"
	defaultQueueName   = "moby-packaging-signing-and-publishing"

	maxErrors = 4
)

var (
	twoMinutesInSeconds int32 = 60 * 2
)

type Message struct {
	Artifact ArtifactInfo `json:"artifact"`
	Spec     archive.Spec `json:"spec"`
}

type ArtifactInfo struct {
	Name      string `json:"name"`
	URI       string `json:"uri"`
	Sha256Sum string `json:"sha256sum"`
}

type Messages struct {
	Messages []*azqueue.DequeuedMessage
	memo     sets.Set[archive.Spec]
}

type Client struct {
	c *azqueue.QueueClient
}

func (c *Client) GetAllMessages(ctx context.Context) (*Messages, error) {
	var (
		max           int32 = 32 // maximum number of messages for request
		failures      int   = 0
		totalFailures int   = 0
		errs          error
		allErrs       error

		dqOpts = azqueue.DequeueMessagesOptions{
			NumberOfMessages:  &max,
			VisibilityTimeout: &twoMinutesInSeconds,
		}

		ret = new(Messages)
	)

	// Temporarily dequeue all the messages to ensure we don't enqueue a duplicate
	for m, err := c.c.DequeueMessages(ctx, &dqOpts); len(m.Messages) != 0; m, err = c.c.DequeueMessages(ctx, &dqOpts) {
		if err != nil {
			errs = errors.Join(errs, err)
			allErrs = errors.Join(allErrs, err)
			totalFailures++
			failures++

			if failures > 4 || totalFailures > 10 {
				fmt.Fprintf(os.Stderr, "##vso[task.logissue type=error;]failed to examine messages: %s\n", errs)
				break
			}
			continue
		}

		ret.Messages = append(ret.Messages, m.Messages...)
		errs = nil
		failures = 0
	}

	if err := ret.memoize(); err != nil {
		allErrs = errors.Join(allErrs, err)
	}

	return ret, allErrs
}

func (m *Messages) memoize() error {
	if len(m.Messages) == 0 {
		return nil
	}

	if m.memo != nil {
		return nil
	}

	m.memo = sets.New[archive.Spec]()

	var errs error

	for i := range m.Messages {
		if m.Messages[i].MessageText == nil {
			errs = errors.Join(errs, fmt.Errorf("nil message %#v", m.Messages[i]))
			continue
		}
		b, err := base64.StdEncoding.DecodeString(*m.Messages[i].MessageText)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}

		var spec archive.Spec
		if err := json.Unmarshal(b, &spec); err != nil {
			errs = errors.Join(errs, err)
			continue
		}

		m.memo.Insert(spec)
	}

	if m.memo.Len() == 0 {
		return fmt.Errorf("failed processing specs in queue message: %w", errs)
	}

	if errs != nil {
		s := strings.Split(errs.Error(), "\n")
		for _, err := range s {
			fmt.Fprintf(os.Stderr, "##vso[task.logissue type=error;]error memoizing queue messages: %s\n", err)
		}
	}

	return nil
}

// used by trigger
func (m *Messages) ContainsBuild(spec archive.Spec) (bool, error) {
	failures := 0
	if len(m.Messages) == 0 {
		return false, nil
	}

	if err := m.memoize(); err != nil {
		return false, err
	}

	for _, rawMessage := range m.Messages {
		if failures > maxErrors {
			return false, fmt.Errorf("too many failures inspecting builds")
		}

		messageID := "unknown"
		if rawMessage.MessageID != nil {
			messageID = *rawMessage.MessageID
		}

		if rawMessage.MessageText == nil {
			failures++
			fmt.Fprintf(os.Stderr, "##vso[task.logissue type=error;]nil message with ID: %s\n", messageID)
			continue
		}

		b, err := base64.StdEncoding.DecodeString(*rawMessage.MessageText)
		if err != nil {
			failures++
			fmt.Fprintf(os.Stderr, "##vso[task.logissue type=error;]error decoding base64 string for message with ID: %s\n", messageID)
			continue
		}

		var m Message
		if err := json.Unmarshal(b, &m); err != nil {
			failures++
			fmt.Fprintf(os.Stderr, "##vso[task.logissue type=error;]error unmarshaling message with ID: %s\n", messageID)
			continue
		}

		if m.Spec == spec {
			return true, nil
		}
	}

	return false, nil
}

func NewDefaultSignQueueClient() (*Client, error) {
	return NewClient(defaultAccountName, defaultQueueName)
}

func NewClient(accountName, queueName string) (*Client, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	serviceURL := fmt.Sprintf("https://%s.queue.core.windows.net", accountName)
	sClient, err := azqueue.NewServiceClient(serviceURL, credential, nil)
	if err != nil {
		return nil, err
	}

	return &Client{c: sClient.NewQueueClient(queueName)}, nil
}
