# Kafka

## 简介

`service_kafka` `input`插件实现了`ServiceInputV1`和`ServiceInputV2`接口，插件用于采集Kafka的消息。

## 版本

[Stable](../../stability-level.md)

## 版本说明

* 推荐版本：LoongCollector v3.0.5 及以上

## 配置参数

| 参数                        | 类型      | 是否必选 | 说明                                                                                                                                          |
|---------------------------|---------|------|---------------------------------------------------------------------------------------------------------------------------------------------|
| Type                      | string  | 是    | 插件类型，指定为`service_kafka`。                                                                                                                    |
| Format                    | string  | 否    | 自 **1.6.0** 起新增（同仓历史版本号，旧文档常写作 ilogtail），仅 ServiceInputV2 支持。v2 版本支持格式：`raw`、`prometheus`、`otlp_metricv1`、`otlp_tracev1`</p><p>说明：`raw` 格式以原始请求字节流传输数据，默认值：`raw`</p> |
| Version                   | string  | 是    | Kafka集群版本号。                                                                                                                                 |
| Brokers                   | Array   | 是    | Kafka服务器地址列表。                                                                                                                               |
| ConsumerGroup             | string  | 是    | Kafka消费组名称。                                                                                                                                 |
| Topics                    | Array   | 是    | 待消费的Kafka订阅主题列表。                                                                                                                            |
| ClientID                  | string  | 是    | 消费Kafka的用户ID。                                                                                                                               |
| Offset                    | string  | 否    | Kafka初始消费位移类型，可选值包括：oldest和newest。如果未添加该参数，则默认使用oldest，表示从最早可用的位移处开始消费。                                                                     |
| MaxMessageLen（Deprecated） | Integer | 否    | Kafka 消息的最大允许长度，单位为字节，取值范围为：1～524288。若未添加该参数，则默认 524288（512KB）。自 **1.6.0** 起已不再使用此参数（旧文档常写作 ilogtail）。                                                      |
| SASLUsername              | string  | 否    | SASL用户名。兼容旧配置，推荐使用 `Authentication.PlainText` 或 `Authentication.SASL`。                                                                                                                                    |
| SASLPassword              | string  | 否    | SASL密码。兼容旧配置，推荐使用 `Authentication.PlainText` 或 `Authentication.SASL`。  |
| Authentication.PlainText.Username | string | 否 | PlainText 认证用户名。 |
| Authentication.PlainText.Password | string | 否 | PlainText 认证密码。 |
| Authentication.SASL.Username | string | 否 | SASL 认证用户名。 |
| Authentication.SASL.Password | string | 否 | SASL 认证密码。 |
| Authentication.SASL.SaslMechanism | string | 否 | SASL 认证机制，可选值：`PLAIN`、`SCRAM-SHA-256`、`SCRAM-SHA-512`。也兼容 `Authentication.Sasl.SaslMechanism` 的历史文档大小写。 |
| Authentication.TLS.Enabled | bool | 否 | 是否启用 TLS 连接，默认 `false`。 |
| Authentication.TLS.CAFile | string | 否 | CA 证书文件路径，用于校验 Kafka 服务端证书。 |
| Authentication.TLS.CertFile | string | 否 | 客户端证书文件路径，和 `Authentication.TLS.KeyFile` 同时配置时用于双向 TLS 认证。 |
| Authentication.TLS.KeyFile | string | 否 | 客户端私钥文件路径，必须和 `Authentication.TLS.CertFile` 同时配置。 |
| Authentication.TLS.InsecureSkipVerify | bool | 否 | 是否跳过服务端证书校验，默认 `false`。生产环境不建议开启。 |
| Authentication.TLS.MinVersion | string | 否 | 最低 TLS 协议版本，可选值：`1.0`、`1.1`、`1.2`、`1.3`，默认 `1.2`。 |
| Authentication.TLS.MaxVersion | string | 否 | 最高 TLS 协议版本，可选值：`1.0`、`1.1`、`1.2`、`1.3`，默认使用 Go 标准库默认值。 |
| Assignor                  | string  | 否    | 消费组消费分区分配策略。可以设置选项：range, roundrobin, sticky，默认值：range                                                                                      |
| DisableUncompress         | bool | 否    | 自 **1.6.0** 起新增（旧文档常写作 ilogtail），禁用对请求数据的解压缩，默认 `false`。<p>目前仅针对 Raw Format 有效。</p><p>仅 v2 版本有效。</p>                                                          |
| FieldsExtend              | bool | 否    | <p>是否支持非integer以外的数据类型(如String)</p><p>目前仅针对有 string、Bool 等额外类型的 influxdb Format 有效，仅v2版本有效</p>                                              |

## 样例

采集服务器地址为172.xx.xx.48和172.xx.xx.34、主题为topicA和topicB的Kafka消息，并将采集结果输出至标准输出，其中Kafka集群的版本为2.1.1，消费组的名称为test-group，其余取默认值。

* 输入

```json
{"payload":"foo"}
```

### 采集配置（v1）

```yaml
enable: true
inputs:
  - Type: service_kafka
    Version: 2.1.1
    Brokers: 
        - 172.xx.xx.48
        - 172.xx.xx.34
    ConsumerGroup: test-group
    Topics:
        - topicA
        - topicB
    ClientID: sls
flushers:
  - Type: flusher_stdout
    OnlyStdout: true  
```

* 输出

```json
{"payload":"foo"}
```

### 采集配置（v2）

`v2` 是自 **1.6.0** 起新增的实现（旧文档常写作 ilogtail），主要支持通过 `Format` 指定 `raw`、`prometheus`、`otlp_metricv1`、`otlp_tracev1` 等数据格式，
其它配置变更可查看【配置参数】表。如果没有特殊的需求，使用默认的`v1`即可。

```yaml
enable: true
version: v2
inputs:
  - Type: service_kafka
    Version: 2.1.1
    Brokers: 
        - 172.xx.xx.48
        - 172.xx.xx.34
    ConsumerGroup: test-group
    Topics:
        - topicA
        - topicB
    ClientID: sls
flushers:
  - Type: flusher_stdout
    OnlyStdout: true  
```

* 输出

```json
{"eventType":"byteArray","name":"","timestamp":0,"observedTimestamp":0,"tags":{},"byteArray":"{\"payload \": \"foo \"}"}
```

### TLS 配置

```yaml
enable: true
inputs:
  - Type: service_kafka
    Version: 2.1.1
    Brokers:
      - 172.xx.xx.48:9093
      - 172.xx.xx.34:9093
    ConsumerGroup: test-group
    Topics:
      - topicA
      - topicB
    ClientID: sls
    Authentication:
      TLS:
        Enabled: true
        CAFile: /etc/kafka/ca.pem
        CertFile: /etc/kafka/client.pem
        KeyFile: /etc/kafka/client-key.pem
        MinVersion: "1.2"
flushers:
  - Type: flusher_stdout
    OnlyStdout: true
```

### SASL/PlainText 配置

```yaml
enable: true
inputs:
  - Type: service_kafka
    Version: 2.1.1
    Brokers:
      - 172.xx.xx.48:9092
      - 172.xx.xx.34:9092
    ConsumerGroup: test-group
    Topics:
      - topicA
      - topicB
    ClientID: sls
    Authentication:
      SASL:
        Username: kafka-user
        Password: kafka-password
        SaslMechanism: SCRAM-SHA-256
flushers:
  - Type: flusher_stdout
    OnlyStdout: true
```
