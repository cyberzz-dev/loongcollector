// Copyright 2023 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	ctx "context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/models"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/pipeline/extensions"
	"github.com/alibaba/ilogtail/pkg/protocol/decoder"
	"github.com/alibaba/ilogtail/pkg/protocol/decoder/common"
	"github.com/alibaba/ilogtail/pkg/selfmonitor"
	"github.com/alibaba/ilogtail/pkg/tlscommon"
)

const (
	v1 = iota
	v2
)

const (
	saslTypePlaintext   = sarama.SASLTypePlaintext
	saslTypeSCRAMSHA256 = sarama.SASLTypeSCRAMSHA256
	saslTypeSCRAMSHA512 = sarama.SASLTypeSCRAMSHA512
)

type InputKafka struct {
	ConsumerGroup  string
	ClientID       string
	Topics         []string
	Brokers        []string
	MaxMessageLen  int
	Version        string
	Offset         string
	SASLUsername   string
	SASLPassword   string
	Authentication Authentication
	// Assignor Consumer group partition assignment strategy (range, roundrobin, sticky)
	Assignor string
	// Decoder the decoder to use, default is "ext_default_decoder"
	Decoder           string
	Format            string
	FieldsExtend      bool
	DisableUncompress bool

	ready               chan bool
	readyCloser         sync.Once
	consumerGroupClient sarama.ConsumerGroup
	wg                  *sync.WaitGroup
	messages            chan *sarama.ConsumerMessage
	context             pipeline.Context
	cancelConsumer      ctx.CancelFunc
	collectorV2         pipeline.PipelineCollector
	decoder             extensions.Decoder
	collectorV1         pipeline.Collector
	version             int8
}

type Authentication struct {
	// PlainText authentication
	PlainText *PlainTextConfig
	// SASL authentication
	SASL *SaslConfig
	// Sasl keeps compatibility with the historical flusher_kafka_v2 document casing.
	Sasl *SaslConfig
	// TLS authentication
	TLS *tlscommon.TLSConfig
}

type PlainTextConfig struct {
	// The username for connecting to Kafka.
	Username string
	// The password for connecting to Kafka.
	Password string
}

type SaslConfig struct {
	// SASL Mechanism to be used, possible values are: (PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512)
	SaslMechanism string
	// The username for connecting to Kafka.
	Username string
	// The password for connecting to Kafka.
	Password string
}

const (
	pluginType = "service_kafka"
)

func (k *InputKafka) Init(context pipeline.Context) (int, error) {
	k.context = context

	if len(k.Brokers) == 0 {
		return 0, fmt.Errorf("must specify Brokers for plugin %v", pluginType)
	}
	if len(k.Topics) == 0 {
		return 0, fmt.Errorf("must specify Topics for plugin %v", pluginType)
	}
	if k.ConsumerGroup == "" {
		return 0, fmt.Errorf("must specify ConsumerGroup for plugin %v", pluginType)
	}
	if k.ClientID == "" {
		return 0, fmt.Errorf("must specify ClientID for plugin %v", pluginType)
	}

	// init decoder
	if k.Format == "" {
		k.Format = common.ProtocolRaw
	}
	var err error
	if k.decoder, err = decoder.GetDecoderWithOptions(k.Format, decoder.Option{
		FieldsExtend:      k.FieldsExtend,
		DisableUncompress: k.DisableUncompress,
	}); err != nil {
		return 0, err
	}

	config, err := k.newSaramaConfig()
	if err != nil {
		return 0, err
	}

	newClient, err := sarama.NewClient(k.Brokers, config)
	if err != nil {
		logger.Warningf(k.context.GetRuntimeContext(), selfmonitor.InputKafkaAlarm, "failed to create kafka client, error: %v", err)
		return 0, err
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(k.ConsumerGroup, newClient)
	if err != nil {
		logger.Warningf(k.context.GetRuntimeContext(), selfmonitor.InputKafkaAlarm, "failed to creating consumer group client, [group]%s", k.ConsumerGroup)
		return 0, err
	}
	k.consumerGroupClient = consumerGroup
	cancelCtx, cancel := ctx.WithCancel(k.context.GetRuntimeContext())
	k.cancelConsumer = cancel
	k.wg = &sync.WaitGroup{}
	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		for {
			if err := k.consumerGroupClient.Consume(cancelCtx, k.Topics, k); err != nil {
				// Keep retrying Consume when error occurs.
				// This loop will only exit when the context is canceled (i.e., when loongcollector process is stopping)
				logger.Warning(k.context.GetRuntimeContext(), selfmonitor.InputKafkaAlarm, "Error from kafka consumer", err)

				// Add a retry delay to avoid busy loop.
				select {
				case <-time.After(time.Second * 5):
				case <-cancelCtx.Done():
					logger.Info(k.context.GetRuntimeContext(), "Consumer was canceled. Leaving consumer group")
					return
				}
			}
			// check if context was canceled, signaling that the consumer should stop
			if cancelCtx.Err() != nil {
				logger.Info(k.context.GetRuntimeContext(), "Consumer was canceled. Leaving consumer group")
				return
			}
			k.ready = make(chan bool)
		}
	}()
	<-k.ready
	return 0, nil
}

func (k *InputKafka) newSaramaConfig() (*sarama.Config, error) {
	config := sarama.NewConfig()
	var err error
	if k.Version != "" {
		if config.Version, err = sarama.ParseKafkaVersion(k.Version); err != nil {
			return nil, err
		}
	}
	config.Consumer.Return.Errors = true

	if k.SASLUsername != "" && k.SASLPassword != "" {
		logger.Infof(k.context.GetRuntimeContext(), "Using SASL auth with username '%s',",
			k.SASLUsername)
		config.Net.SASL.User = k.SASLUsername
		config.Net.SASL.Password = k.SASLPassword
		config.Net.SASL.Enable = true
	}

	if err := k.Authentication.ConfigureAuthentication(config); err != nil {
		return nil, err
	}

	switch strings.ToLower(k.Offset) {
	case "oldest", "":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		logger.Warningf(k.context.GetRuntimeContext(), selfmonitor.InputKafkaAlarm, "Kafka consumer invalid offset '%s', using 'oldest'",
			k.Offset)
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	switch strings.ToLower(k.Assignor) {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	case "range":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	default:
		logger.Warningf(k.context.GetRuntimeContext(), selfmonitor.InputKafkaAlarm, "Unrecognized consumer group partition assignor '%s', using 'oldest'",
			k.Assignor)
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRange}
	}
	return config, nil
}

func (config *Authentication) ConfigureAuthentication(saramaConfig *sarama.Config) error {
	if config.PlainText != nil {
		if err := config.PlainText.ConfigurePlaintext(saramaConfig); err != nil {
			return err
		}
	}

	saslConfig := config.SASL
	if saslConfig == nil {
		saslConfig = config.Sasl
	} else if config.Sasl != nil && saslConfig.SaslMechanism == "" {
		saslConfig.SaslMechanism = config.Sasl.SaslMechanism
	}
	if saslConfig != nil {
		if err := saslConfig.ConfigureSasl(saramaConfig); err != nil {
			return err
		}
	}

	if config.TLS != nil {
		if err := configureTLS(config.TLS, saramaConfig); err != nil {
			return err
		}
	}
	return nil
}

func (plainTextConfig *PlainTextConfig) ConfigurePlaintext(saramaConfig *sarama.Config) error {
	if plainTextConfig.Username != "" && plainTextConfig.Password == "" {
		return fmt.Errorf("PlainTextConfig password must be set when username is configured")
	}

	if plainTextConfig.Username != "" {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.User = plainTextConfig.Username
		saramaConfig.Net.SASL.Password = plainTextConfig.Password
	}
	return nil
}

func (saslConfig *SaslConfig) ConfigureSasl(saramaConfig *sarama.Config) error {
	if saslConfig.Username == "" {
		return fmt.Errorf("username have to be provided")
	}

	if saslConfig.Password == "" {
		return fmt.Errorf("password have to be provided")
	}

	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.User = saslConfig.Username
	saramaConfig.Net.SASL.Password = saslConfig.Password
	switch strings.ToUpper(saslConfig.SaslMechanism) {
	case "":
	case saslTypePlaintext:
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case saslTypeSCRAMSHA256:
		saramaConfig.Net.SASL.Handshake = true
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
		}
	case saslTypeSCRAMSHA512:
		saramaConfig.Net.SASL.Handshake = true
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
	default:
		return fmt.Errorf("not valid SASL mechanism '%v', only supported with PLAIN|SCRAM-SHA-512|SCRAM-SHA-256", saslConfig.SaslMechanism)
	}
	return nil
}

func configureTLS(config *tlscommon.TLSConfig, saramaConfig *sarama.Config) error {
	tlsConfig, err := config.LoadTLSConfig()
	if err != nil {
		return fmt.Errorf("error loading tls config: %w", err)
	}
	if tlsConfig != nil {
		saramaConfig.Net.TLS.Enable = true
		saramaConfig.Net.TLS.Config = tlsConfig
	}
	return nil
}

func (k *InputKafka) Description() string {
	return "Kafka input for logtail"
}

func (k *InputKafka) Start(collector pipeline.Collector) error {
	k.collectorV1 = collector
	k.version = v1
	go func() {
		k.receiver()
	}()
	return nil
}

func (k *InputKafka) StartService(context pipeline.PipelineContext) error {
	k.collectorV2 = context.Collector()
	k.version = v2
	go func() {
		k.receiver()
	}()
	return nil
}

func (k *InputKafka) receiver() {
	for msg := range k.messages {
		k.onMessage(msg)
	}
}

// Setup implements ConsumerGroupHandler
func (k *InputKafka) Setup(session sarama.ConsumerGroupSession) error {
	k.readyCloser.Do(func() {
		close(k.ready)
	})
	return nil
}

// Cleanup implements ConsumerGroupHandler
func (k *InputKafka) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements ConsumerGroupHandler, must start a consumer loop of ConsumerGroupClaim's Messages().
func (k *InputKafka) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	logger.Debug(k.context.GetRuntimeContext(), "Consuming messages [partition]", claim.Partition(), "[topic]", claim.Topic(),
		"init [offset]", claim.InitialOffset())
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			k.messages <- msg
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			logger.Debug(k.context.GetRuntimeContext(), "Ctx was canceled, stopping consumerGroup")
			return nil
		}
	}
}

func (k *InputKafka) onMessage(msg *sarama.ConsumerMessage) {
	if msg != nil {
		switch k.version {
		case v1:
			fields := make(map[string]string)
			if len(msg.Key) == 0 {
				fields[models.ContentKey] = string(msg.Value)
			} else {
				fields[string(msg.Key)] = string(msg.Value)
			}
			k.collectorV1.AddData(nil, fields)
		case v2:
			data, err := k.decoder.DecodeV2(msg.Value, nil)
			if err != nil {
				logger.Warning(k.context.GetRuntimeContext(), selfmonitor.DecodeMessageFailAlarm, "decode message failed", err)
				return
			}
			k.collectorV2.CollectList(data...)
		}
	}
}

func (k *InputKafka) Stop() error {
	k.readyCloser.Do(func() {
		close(k.ready)
	})
	k.cancelConsumer()
	k.wg.Wait()
	err := k.consumerGroupClient.Close()
	if err != nil {
		e := fmt.Errorf("[inputs.kafka_consumer] Error closing consumer: %v", err)
		logger.Warningf(k.context.GetRuntimeContext(), selfmonitor.InputKafkaAlarm, "%v", e)
		return e
	}
	return nil
}

func (k *InputKafka) Collect(collector pipeline.Collector) error {
	return nil
}

func init() {
	pipeline.ServiceInputs[pluginType] = func() pipeline.ServiceInput {
		return &InputKafka{
			ConsumerGroup: "",
			ClientID:      "",
			Topics:        nil,
			Brokers:       nil,
			Version:       "",
			Offset:        "oldest",
			SASLUsername:  "",
			SASLPassword:  "",
			Assignor:      "range",
			messages:      make(chan *sarama.ConsumerMessage, 256),
			ready:         make(chan bool),
		}
	}
}
