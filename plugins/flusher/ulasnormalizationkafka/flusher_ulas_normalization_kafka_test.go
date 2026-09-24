// Copyright 2026 iLogtail Authors
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

package ulasnormalizationkafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alibaba/ilogtail/pkg/models"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/stretchr/testify/require"
)

func TestExportFormattingProducesStrictUlasNormalizationRecord(t *testing.T) {
	flusher := NewFlusherUlasNormalizationKafka()
	require.NoError(t, flusher.initTimeLocation())

	timestamp := time.Date(2026, 9, 23, 13, 4, 44, 0, time.UTC).UnixNano()
	log := models.NewSimpleLog([]byte("================  Request Start  ================="),
		models.NewTagsWithMap(map[string]string{"host.ip": "10.81.135.33"}), uint64(timestamp))
	log.GetIndices().Add("datasourceId", "2096852199859093506")
	log.GetIndices().Add("datasourceName", "数据源c1fa6871-8e64-43db-b7de-e303601a25db")
	log.GetIndices().Add("templateId", "2054459655839100930")
	group := &models.PipelineGroupEvents{
		Group: models.NewGroup(models.NewMetadata(), models.NewTagsWithMap(map[string]string{
			"host.name":     "ulas-master",
			"log.file.path": "/data/docker/csf/startup/log/csf-rules/csf-rules-info.log",
		})),
		Events: []models.PipelineEvent{log},
	}

	normalized, err := flusher.normalizeGroupForExport(group)
	require.NoError(t, err)
	require.Len(t, normalized.Events, 1)

	var actual map[string]string
	require.NoError(t, json.Unmarshal(normalized.Events[0].(models.ByteArray), &actual))
	require.Equal(t, map[string]string{
		"content":        "================  Request Start  =================",
		"hostIp":         "10.81.135.33",
		"hostName":       "ulas-master",
		"logFilePath":    "/data/docker/csf/startup/log/csf-rules/csf-rules-info.log",
		"datasourceId":   "2096852199859093506",
		"datasourceName": "数据源c1fa6871-8e64-43db-b7de-e303601a25db",
		"templateId":     "2054459655839100930",
		"pollTime":       "2026-09-23 21:04:44",
	}, actual)
	require.Len(t, actual, 8)
}

func TestExportFormattingDropsRecordMissingRequiredMetadata(t *testing.T) {
	flusher := NewFlusherUlasNormalizationKafka()
	require.NoError(t, flusher.initTimeLocation())
	group := &models.PipelineGroupEvents{
		Group: models.NewGroup(models.NewMetadata(), models.NewTagsWithMap(map[string]string{
			"host.ip":       "10.81.135.33",
			"host.name":     "ulas-master",
			"log.file.path": "/data/app.log",
		})),
		Events: []models.PipelineEvent{models.NewSimpleLog([]byte("message"), models.NewTags(), uint64(time.Now().UnixNano()))},
	}

	normalized, err := flusher.normalizeGroupForExport(group)
	require.NoError(t, err)
	require.Nil(t, normalized)
	require.Equal(t, uint64(1), flusher.droppedRecords.Load())
}

func TestFormatLegacyLogGroupProducesTheSameUlasContract(t *testing.T) {
	flusher := NewFlusherUlasNormalizationKafka()
	require.NoError(t, flusher.initTimeLocation())
	legacyLogGroup := &protocol.LogGroup{
		Source: "10.81.135.33",
		LogTags: []*protocol.LogTag{
			{Key: "__hostname__", Value: "ulas-master"},
			{Key: "__path__", Value: "/data/csf-rules-info.log"},
		},
		Logs: []*protocol.Log{{
			Time: uint32(time.Date(2026, 9, 23, 13, 4, 44, 0, time.UTC).Unix()),
			Contents: []*protocol.Log_Content{
				{Key: "content", Value: "legacy message"},
				{Key: "datasourceId", Value: "2096852199859093506"},
				{Key: "datasourceName", Value: "数据源"},
				{Key: "templateId", Value: "2054459655839100930"},
			},
		}},
	}

	group, err := flusher.normalizeLegacyLogGroup(legacyLogGroup)
	require.NoError(t, err)
	require.Len(t, group.Events, 1)
	var actual map[string]string
	require.NoError(t, json.Unmarshal(group.Events[0].(models.ByteArray), &actual))
	require.Equal(t, "10.81.135.33", actual["hostIp"])
	require.Equal(t, "ulas-master", actual["hostName"])
	require.Equal(t, "/data/csf-rules-info.log", actual["logFilePath"])
	require.Equal(t, "2026-09-23 21:04:44", actual["pollTime"])
	require.Len(t, actual, 8)
}

func TestRegistersUlasNormalizationKafkaFlusher(t *testing.T) {
	creator, exists := pipeline.Flushers[pluginType]
	require.True(t, exists)
	require.IsType(t, &FlusherUlasNormalizationKafka{}, creator())
}

func TestConfigurationKeepsKafkaConnectionSettingsOnEmbeddedFlusher(t *testing.T) {
	flusher := NewFlusherUlasNormalizationKafka()
	require.NoError(t, json.Unmarshal([]byte(`{
        "Brokers": ["kafka-1:9092", "kafka-2:9092"],
        "Topic": "usiem-normalization-topic",
        "TimeZone": "Asia/Shanghai",
        "TimeFormat": "2006-01-02 15:04:05"
    }`), flusher))

	require.Equal(t, []string{"kafka-1:9092", "kafka-2:9092"}, flusher.Brokers)
	require.Equal(t, "usiem-normalization-topic", flusher.Topic)
	require.Equal(t, "Asia/Shanghai", flusher.TimeZone)
	require.Equal(t, "2006-01-02 15:04:05", flusher.TimeFormat)
}
