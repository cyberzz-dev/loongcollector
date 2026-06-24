// Copyright 2021 iLogtail Authors
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
	"bytes"
	"crypto/tls"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alibaba/ilogtail/pkg/helper"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/pkg/tlscommon"
	pluginmanager "github.com/alibaba/ilogtail/pluginmanager"
)

type ContextTest struct {
	pluginmanager.ContextImp

	Logs []string
}

func (p *ContextTest) LogWarnf(alarmType string, format string, params ...interface{}) {
	p.Logs = append(p.Logs, alarmType+":"+fmt.Sprintf(format, params...))
}

func (p *ContextTest) LogErrorf(alarmType string, format string, params ...interface{}) {
	p.Logs = append(p.Logs, alarmType+":"+fmt.Sprintf(format, params...))
}

func newInput() (*ContextTest, *InputKafka, error) {
	ctx := &ContextTest{}
	processor := &InputKafka{
		Brokers:       []string{"localhost:9092"},
		Topics:        []string{"test1"},
		ConsumerGroup: "group1",
		ClientID:      "id1",
		Offset:        "oldest",
		MaxMessageLen: 9,
	}
	_, err := processor.Init(&ctx.ContextImp)
	return ctx, processor, err
}

func TestNewSaramaConfigWithTLS(t *testing.T) {
	input := &InputKafka{
		Offset:   "oldest",
		Assignor: "range",
		Authentication: Authentication{
			TLS: &tlscommon.TLSConfig{
				Enabled:            true,
				InsecureSkipVerify: true,
				MinVersion:         "1.2",
				MaxVersion:         "1.3",
			},
		},
	}

	config, err := input.newSaramaConfig()
	require.NoError(t, err)
	require.True(t, config.Net.TLS.Enable)
	require.NotNil(t, config.Net.TLS.Config)
	assert.True(t, config.Net.TLS.Config.InsecureSkipVerify)
	assert.Equal(t, uint16(tls.VersionTLS12), config.Net.TLS.Config.MinVersion)
	assert.Equal(t, uint16(tls.VersionTLS13), config.Net.TLS.Config.MaxVersion)
}

func TestNewSaramaConfigWithTLSDisabled(t *testing.T) {
	input := &InputKafka{
		Offset:   "oldest",
		Assignor: "range",
		Authentication: Authentication{
			TLS: &tlscommon.TLSConfig{},
		},
	}

	config, err := input.newSaramaConfig()
	require.NoError(t, err)
	assert.False(t, config.Net.TLS.Enable)
	assert.Nil(t, config.Net.TLS.Config)
}

func TestNewSaramaConfigWithInvalidTLS(t *testing.T) {
	input := &InputKafka{
		Offset:   "oldest",
		Assignor: "range",
		Authentication: Authentication{
			TLS: &tlscommon.TLSConfig{
				Enabled:  true,
				CertFile: "client.crt",
			},
		},
	}

	_, err := input.newSaramaConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "either both certificate and key must be supplied")
}

func TestNewSaramaConfigWithPlainTextAuthentication(t *testing.T) {
	input := &InputKafka{
		Offset:   "oldest",
		Assignor: "range",
		Authentication: Authentication{
			PlainText: &PlainTextConfig{
				Username: "user",
				Password: "pass",
			},
		},
	}

	config, err := input.newSaramaConfig()
	require.NoError(t, err)
	assert.True(t, config.Net.SASL.Enable)
	assert.Equal(t, "user", config.Net.SASL.User)
	assert.Equal(t, "pass", config.Net.SASL.Password)
}

func TestNewSaramaConfigWithPlainTextMissingPassword(t *testing.T) {
	input := &InputKafka{
		Offset:   "oldest",
		Assignor: "range",
		Authentication: Authentication{
			PlainText: &PlainTextConfig{
				Username: "user",
			},
		},
	}

	_, err := input.newSaramaConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PlainTextConfig password must be set")
}

func TestNewSaramaConfigWithSASLPlain(t *testing.T) {
	input := &InputKafka{
		Offset:   "oldest",
		Assignor: "range",
		Authentication: Authentication{
			SASL: &SaslConfig{
				SaslMechanism: "PLAIN",
				Username:      "user",
				Password:      "pass",
			},
		},
	}

	config, err := input.newSaramaConfig()
	require.NoError(t, err)
	assert.True(t, config.Net.SASL.Enable)
	assert.Equal(t, sarama.SASLTypePlaintext, string(config.Net.SASL.Mechanism))
	assert.Equal(t, "user", config.Net.SASL.User)
	assert.Equal(t, "pass", config.Net.SASL.Password)
}

func TestNewSaramaConfigWithSASLSCRAM(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		expected  string
	}{
		{
			name:      "sha256",
			mechanism: "SCRAM-SHA-256",
			expected:  sarama.SASLTypeSCRAMSHA256,
		},
		{
			name:      "sha512",
			mechanism: "SCRAM-SHA-512",
			expected:  sarama.SASLTypeSCRAMSHA512,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &InputKafka{
				Offset:   "oldest",
				Assignor: "range",
				Authentication: Authentication{
					SASL: &SaslConfig{
						SaslMechanism: tt.mechanism,
						Username:      "user",
						Password:      "pass",
					},
				},
			}

			config, err := input.newSaramaConfig()
			require.NoError(t, err)
			assert.True(t, config.Net.SASL.Enable)
			assert.True(t, config.Net.SASL.Handshake)
			assert.Equal(t, tt.expected, string(config.Net.SASL.Mechanism))
			require.NotNil(t, config.Net.SASL.SCRAMClientGeneratorFunc)
		})
	}
}

func TestNewSaramaConfigWithSASLMechanismAlias(t *testing.T) {
	input := &InputKafka{
		Offset:   "oldest",
		Assignor: "range",
		Authentication: Authentication{
			SASL: &SaslConfig{
				Username: "user",
				Password: "pass",
			},
			Sasl: &SaslConfig{
				SaslMechanism: "SCRAM-SHA-256",
			},
		},
	}

	config, err := input.newSaramaConfig()
	require.NoError(t, err)
	assert.Equal(t, sarama.SASLTypeSCRAMSHA256, string(config.Net.SASL.Mechanism))
	require.NotNil(t, config.Net.SASL.SCRAMClientGeneratorFunc)
}

func TestNewSaramaConfigWithInvalidSASL(t *testing.T) {
	input := &InputKafka{
		Offset:   "oldest",
		Assignor: "range",
		Authentication: Authentication{
			SASL: &SaslConfig{
				SaslMechanism: "SCRAM-SHA-1",
				Username:      "user",
				Password:      "pass",
			},
		},
	}

	_, err := input.newSaramaConfig()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not valid SASL mechanism")
}

type mockLog struct {
	tags   map[string]string
	fields map[string]string
}

type mockCollector struct {
	logs []*mockLog
}

func (c *mockCollector) AddData(
	tags map[string]string, fields map[string]string, t ...time.Time) {
	c.AddDataWithContext(tags, fields, nil, t...)
}

func (c *mockCollector) AddDataArray(
	tags map[string]string, columns []string, values []string, t ...time.Time) {
	c.AddDataArrayWithContext(tags, columns, values, nil, t...)
}

func (c *mockCollector) AddRawLog(log *protocol.Log) {
	c.AddRawLogWithContext(log, nil)
}

func (c *mockCollector) AddDataWithContext(
	tags map[string]string, fields map[string]string, ctx map[string]interface{}, t ...time.Time) {
	c.logs = append(c.logs, &mockLog{tags, fields})
}

func (c *mockCollector) AddDataArrayWithContext(
	tags map[string]string, columns []string, values []string, ctx map[string]interface{}, t ...time.Time) {
}

func (c *mockCollector) AddRawLogWithContext(log *protocol.Log, ctx map[string]interface{}) {

}

// Invalid Test
func InvalidTestInputKafka(t *testing.T) {
	initKafka()
	_, input, err := newInput()
	require.NoError(t, err)
	collector := &mockCollector{}
	pipelineCxt := helper.NewGroupedPipelineContext()
	err = input.StartService(pipelineCxt)
	require.NoError(t, err)
	time.Sleep(time.Second * 2)
	assert.Equal(t, 2, len(collector.logs))
	log := collector.logs[0]
	println(log.fields["key1"])
	assert.Equal(t, "value1", log.fields["key1"])

	log = collector.logs[1]
	println(log.fields["key2"])
	assert.Equal(t, "value2", log.fields["key2"])

	require.NoError(t, input.Stop())
	closeKafka()
}

//nolint:unparam
func execShell(args string) (string, error) {
	cmd := exec.Command("/bin/bash", "-c", args) //nolint:gosec
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	return out.String(), err
}

func initKafka() {
	// _, _ = execShell("zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties")
	// println("zookeeper-server-start")
	// _, _ = execShell("kafka-server-start /usr/local/etc/kafka/server.properties")
	// println("kafka-server-start")
	// _, _ = execShell("./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test1")
	// println("kafka-topics create")
	_, _ = execShell("kafka-console-producer -brokers localhost:9092 -topic test1 -key key1 -value value1")
	println("kafka-console-producer key1")
	_, _ = execShell("kafka-console-producer -brokers localhost:9092 -topic test1 -key key2 -value value2")
	println("kafka-console-producer key2")
	_, _ = execShell("kafka-console-producer -brokers localhost:9092 -topic test1 -key key3 -value 0123456789")
	println("kafka-console-producer key3")

}

func closeKafka() {
	// _, _ = execShell("./kafka-topics.sh --delete --zookeeper localhost:2181 --topic test1")
	// _, _ = execShell("kafka-server-stop")
	// _, _ = execShell("zookeeper-server-stop")
}
