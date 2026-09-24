# ULAS 归一化 Kafka 输出

`flusher_ulas_normalization_kafka` 用于 ULAS Agent 日志采集。它复用 `flusher_kafka_v2` 的 Kafka 连接、认证、重试和批量发送能力，但输出格式固定为 ULAS 归一化 Topic 的 JSON 契约，不能作为通用 Kafka 输出替代品。

## 配置

```yaml
processors:
  - Type: processor_add_fields
    Fields:
      datasourceId: '2096852199859093506'
      datasourceName: '数据源c1fa6871-8e64-43db-b7de-e303601a25db'
      templateId: '2054459655839100930'
flushers:
  - Type: flusher_ulas_normalization_kafka
    Brokers:
      - '10.81.135.44:39092'
    Topic: 'usiem-normalization-topic'
    TimeZone: 'Asia/Shanghai'
    TimeFormat: '2006-01-02 15:04:05'
```

`Brokers`、`Topic` 及认证、重试、压缩等 Kafka 参数与 `flusher_kafka_v2` 一致。`TimeZone` 默认为 `Asia/Shanghai`，`TimeFormat` 默认为 `2006-01-02 15:04:05`，均采用 Go 的时区名和时间 layout 语法。

## 输出格式

每条日志只输出以下八个字段，ID 始终为字符串：

```json
{
  "content": "================  Request Start  ================",
  "hostIp": "10.81.135.33",
  "hostName": "ulas-master",
  "logFilePath": "/data/docker/csf/startup/log/csf-rules/csf-rules-info.log",
  "datasourceId": "2096852199859093506",
  "datasourceName": "数据源c1fa6871-8e64-43db-b7de-e303601a25db",
  "templateId": "2054459655839100930",
  "pollTime": "2026-09-23 13:04:44"
}
```

- `content`：日志正文。
- `hostIp`、`hostName`、`logFilePath`：来自 `host.ip`、`host.name`、`log.file.path` 标签。
- `datasourceId`、`datasourceName`、`templateId`：来自日志 contents；ULAS 平台通过 `processor_add_fields` 注入，不能由页面传入。
- `pollTime`：事件本身的时间按配置格式化，绝不使用发送时的当前时间。

任一字段缺失时，该条消息不会发送；插件会记录 `FlusherFlushAlarm` 和递增的丢弃计数。V1 与 V2 Go pipeline 都使用同一契约。
