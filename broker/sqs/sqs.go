package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
)

// SQSBroker is a broker backed by SQS
type SQSBroker struct {
	client    *sqs.SQS
	queues    map[string]string
	mu        sync.RWMutex
	tasksChan chan broker.TaskAction
	stop      chan any
	region    string
	prefix    string
}

// Config defines the configuration for SQS
type Config struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Endpoint        string
	Prefix          string
}

// NewSQSBroker creates a new SQS broker
func NewSQSBroker(cfg Config) (broker.Broker, error) {
	awsCfg := aws.NewConfig().WithRegion(cfg.Region)
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		awsCfg = awsCfg.WithCredentials(credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken))
	}
	if cfg.Endpoint != "" {
		awsCfg = awsCfg.WithEndpoint(cfg.Endpoint)
	}
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		return nil, errors.Wrap(err, "error creating SQS session")
	}
	return &SQSBroker{
		client:    sqs.New(sess),
		queues:    make(map[string]string),
		tasksChan: make(chan broker.TaskAction),
		stop:      make(chan any),
		region:    cfg.Region,
		prefix:    cfg.Prefix,
	}, nil
}

func (b *SQSBroker) Queues(ctx context.Context) ([]broker.QueueInfo, error) {
	result, err := b.client.ListQueuesWithContext(ctx, &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String(b.prefix),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error listing queues")
	}
	infos := make([]broker.QueueInfo, len(result.QueueUrls))
	for i, qurl := range result.QueueUrls {
		qname := qurl[strings.LastIndex(*qurl, "/")+1:]
		// TODO: SQS does not readily provide message/consumer counts per queue.
		// This would require additional calls or CloudWatch metrics.
		infos[i] = broker.QueueInfo{
			Name:        qname,
			Messages:    0, // Placeholder
			Consumers:   0, // Placeholder
			Unacked:     0, // Placeholder
			BrokerName:  broker.BROKER_SQS,
			Region:      b.region,
			TaskActions: b.tasksChan,
		}
	}
	return infos, nil
}

func (b *SQSBroker) PublishTask(ctx context.Context, qname string, t *tork.Task) error {
	qurl, err := b.getQueueURL(ctx, qname)
	if err != nil {
		return err
	}
	body, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "error marshalling task %s to json", t.ID)
	}
	_, err = b.client.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(qurl),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return errors.Wrapf(err, "error sending task %s to queue %s", t.ID, qname)
	}
	return nil
}

func (b *SQSBroker) SubscribeForTasks(ctx context.Context, qname string, handler func(t *tork.Task) error) error {
	qurl, err := b.getQueueURL(ctx, qname)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msgf("Context cancelled. Stopping SQS poller for %s", qname)
				return
			case <-b.stop:
				log.Info().Msgf("Broker stopping. Stopping SQS poller for %s", qname)
				return
			default:
				out, err := b.client.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
					QueueUrl:            aws.String(qurl),
					MaxNumberOfMessages: aws.Int64(1),
					WaitTimeSeconds:     aws.Int64(20), // Long polling
				})
				if err != nil {
					log.Error().Err(err).Msgf("error receiving messages from %s", qname)
					// Backoff before retrying
					time.Sleep(time.Second * 5)
					continue
				}
				for _, msg := range out.Messages {
					var t tork.Task
					if err := json.Unmarshal([]byte(*msg.Body), &t); err != nil {
						log.Error().Err(err).Msgf("error unmarshalling task from SQS message")
						// TODO: consider moving to DLQ
						b.deleteMessage(ctx, qurl, msg.ReceiptHandle) // Best effort delete
						continue
					}
					if err := handler(&t); err == nil {
						if err := b.deleteMessage(ctx, qurl, msg.ReceiptHandle); err != nil {
							log.Error().Err(err).Msgf("error deleting message from SQS queue %s", qname)
						}
					} else {
						log.Error().Err(err).Msgf("error handling task %s", t.ID)
						// Consider not deleting the message if handler fails, to allow for retries
						// or move to a dead-letter queue. For now, we delete.
						b.deleteMessage(ctx, qurl, msg.ReceiptHandle) // Best effort delete
					}
				}
			}
		}
	}()
	return nil
}

func (b *SQSBroker) PublishHeartbeat(ctx context.Context, n *tork.Node) error {
	// SQS is not typically used for heartbeats in the same way as RabbitMQ/Redis.
	// This could be implemented using a separate queue or a different AWS service (e.g., SNS, CloudWatch Events).
	// For now, this is a no-op.
	log.Debug().Msgf("PublishHeartbeat is a no-op for SQS broker")
	return nil
}

func (b *SQSBroker) SubscribeForHeartbeats(ctx context.Context, handler func(n *tork.Node)) error {
	// See PublishHeartbeat. This is a no-op for SQS.
	log.Debug().Msgf("SubscribeForHeartbeats is a no-op for SQS broker")
	return nil
}

func (b *SQSBroker) PublishJob(ctx context.Context, qname string, j *tork.Job) error {
	qurl, err := b.getQueueURL(ctx, qname)
	if err != nil {
		return err
	}
	body, err := json.Marshal(j)
	if err != nil {
		return errors.Wrapf(err, "error marshalling job %s to json", j.ID)
	}
	_, err = b.client.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(qurl),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		return errors.Wrapf(err, "error sending job %s to queue %s", j.ID, qname)
	}
	return nil
}

func (b *SQSBroker) SubscribeForJobs(ctx context.Context, qname string, handler func(j *tork.Job) error) error {
	qurl, err := b.getQueueURL(ctx, qname)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Info().Msgf("Context cancelled. Stopping SQS poller for %s", qname)
				return
			case <-b.stop:
				log.Info().Msgf("Broker stopping. Stopping SQS poller for %s", qname)
				return
			default:
				out, err := b.client.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
					QueueUrl:            aws.String(qurl),
					MaxNumberOfMessages: aws.Int64(1),
					WaitTimeSeconds:     aws.Int64(20),
				})
				if err != nil {
					log.Error().Err(err).Msgf("error receiving job messages from %s", qname)
					time.Sleep(time.Second * 5)
					continue
				}
				for _, msg := range out.Messages {
					var j tork.Job
					if err := json.Unmarshal([]byte(*msg.Body), &j); err != nil {
						log.Error().Err(err).Msgf("error unmarshalling job from SQS message")
						b.deleteMessage(ctx, qurl, msg.ReceiptHandle)
						continue
					}
					if err := handler(&j); err == nil {
						if err := b.deleteMessage(ctx, qurl, msg.ReceiptHandle); err != nil {
							log.Error().Err(err).Msgf("error deleting message from SQS queue %s", qname)
						}
					} else {
						log.Error().Err(err).Msgf("error handling job %s", j.ID)
						b.deleteMessage(ctx, qurl, msg.ReceiptHandle)
					}
				}
			}
		}
	}()
	return nil
}

func (b *SQSBroker) Shutdown(ctx context.Context) error {
	log.Info().Msg("Shutting down SQS broker")
	close(b.stop)
	// Allow some time for pollers to stop gracefully
	time.Sleep(time.Millisecond * 100)
	return nil
}

func (b *SQSBroker) getQueueURL(ctx context.Context, qname string) (string, error) {
	b.mu.RLock()
	qurl, ok := b.queues[qname]
	b.mu.RUnlock()
	if ok {
		return qurl, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// double check
	qurl, ok = b.queues[qname]
	if ok {
		return qurl, nil
	}

	fqn := b.prefixQueueName(qname)
	result, err := b.client.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(fqn),
	})
	if err != nil {
		// If queue not found, create it
		var awsErr aws.Error
		if errors.As(err, &awsErr) && awsErr.Code() == sqs.ErrCodeQueueDoesNotExist {
			log.Info().Msgf("Queue %s does not exist. Creating it now.", fqn)
			createResult, createErr := b.client.CreateQueueWithContext(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(fqn),
				Attributes: map[string]*string{
					// FifoQueue: aws.String("false"), // Standard queue
					// ContentBasedDeduplication: aws.String("false"),
					// MessageRetentionPeriod: aws.String("345600"), // 4 days
				},
			})
			if createErr != nil {
				return "", errors.Wrapf(createErr, "error creating queue %s", fqn)
			}
			b.queues[qname] = *createResult.QueueUrl
			return *createResult.QueueUrl, nil
		}
		return "", errors.Wrapf(err, "error getting queue URL for %s", fqn)
	}
	b.queues[qname] = *result.QueueUrl
	return *result.QueueUrl, nil
}

func (b *SQSBroker) deleteMessage(ctx context.Context, qurl string, receiptHandle *string) error {
	_, err := b.client.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(qurl),
		ReceiptHandle: receiptHandle,
	})
	return err
}

func (b *SQSBroker) prefixQueueName(qname string) string {
	if b.prefix == "" {
		return qname
	}
	if strings.HasPrefix(qname, b.prefix) {
		return qname
	}
	return fmt.Sprintf("%s%s", b.prefix, qname)
}

func (b *SQSBroker) HealthCheck(ctx context.Context) (map[string]string, error) {
	status := make(map[string]string)
	// Attempt to list queues as a basic health check
	_, err := b.client.ListQueuesWithContext(ctx, &sqs.ListQueuesInput{MaxResults: aws.Int64(1)})
	if err != nil {
		status["status"] = "DOWN"
		status["error"] = err.Error()
		return status, err
	}
	status["status"] = "UP"
	return status, nil
}

func (b *SQSBroker) SubscribeTaskActions(ctx context.Context, taskID string, handler func(action broker.TaskAction)) error {
	// This is not a typical SQS pattern. Task actions might be better handled
	// through direct API calls or a different messaging system if real-time updates are critical.
	// For simplicity, we'll use the existing tasksChan which is not specific to a taskID.
	// A more robust solution would involve a separate mechanism.
	log.Warn().Msgf("SubscribeTaskActions for SQS is rudimentary and not task-specific.")
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-b.stop:
				return
			case action := <-b.tasksChan:
				// Rudimentary filter - ideally, SQS messages would target specific task IDs
				// or a different architecture would be used for this.
				if action.TaskID == taskID || taskID == "" { // Empty taskID means subscribe to all
					handler(action)
				}
			}
		}
	}()
	return nil
}

func (b *SQSBroker) PublishTaskAction(ctx context.Context, taskID string, actionType broker.TaskActionType, result *tork.TaskResult) error {
	// This would typically involve sending a message to a specific queue/topic listened to by interested parties.
	// For this example, we'll push to an internal channel. This is not scalable for distributed systems.
	log.Debug().Msgf("PublishTaskAction called for task %s with action %s. This is a local notification for SQS.", taskID, actionType)
	select {
	case b.tasksChan <- broker.TaskAction{TaskID: taskID, Type: actionType, Result: result}:
	default:
		log.Warn().Msgf("Could not publish task action for %s: channel buffer full or no active listeners.", taskID)
	}
	return nil
}
