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

// Package ulasnormalizationkafka provides the fixed ULAS normalization Kafka output.
package ulasnormalizationkafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/models"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/pkg/selfmonitor"
	"github.com/alibaba/ilogtail/plugins/flusher/kafkav2"
)

const (
	pluginType        = "flusher_ulas_normalization_kafka"
	defaultTimeZone   = "Asia/Shanghai"
	defaultTimeFormat = "2006-01-02 15:04:05"
)

// FlusherUlasNormalizationKafka serializes each collected log into the ULAS
// normalization contract before delegating Kafka delivery to FlusherKafka.
type FlusherUlasNormalizationKafka struct {
	*kafkav2.FlusherKafka

	// TimeZone is an IANA location name used to format pollTime.
	TimeZone string
	// TimeFormat follows Go's time layout syntax and defaults to a second-level timestamp.
	TimeFormat string

	context        pipeline.Context
	location       *time.Location
	droppedRecords atomic.Uint64
}

type normalizationRecord struct {
	Content        string `json:"content"`
	HostIP         string `json:"hostIp"`
	HostName       string `json:"hostName"`
	LogFilePath    string `json:"logFilePath"`
	DatasourceID   string `json:"datasourceId"`
	DatasourceName string `json:"datasourceName"`
	TemplateID     string `json:"templateId"`
	PollTime       string `json:"pollTime"`
}

// NewFlusherUlasNormalizationKafka returns a flusher with the ULAS timestamp defaults.
func NewFlusherUlasNormalizationKafka() *FlusherUlasNormalizationKafka {
	return &FlusherUlasNormalizationKafka{
		FlusherKafka: kafkav2.NewFlusherKafka(),
		TimeZone:     defaultTimeZone,
		TimeFormat:   defaultTimeFormat,
	}
}

// Init validates the ULAS-specific configuration, then initializes the shared Kafka producer.
func (f *FlusherUlasNormalizationKafka) Init(context pipeline.Context) error {
	f.context = context
	if err := f.initTimeLocation(); err != nil {
		return fmt.Errorf("init ULAS normalization Kafka flusher: %w", err)
	}
	f.FlusherKafka.UseRawV2Converter()
	return f.FlusherKafka.Init(context)
}

// Description returns the plugin description.
func (*FlusherUlasNormalizationKafka) Description() string {
	return "ULAS normalization Kafka flusher for logtail"
}

// Flush keeps the V1 pipeline compatible with the same ULAS output contract.
func (f *FlusherUlasNormalizationKafka) Flush(_ string, _ string, _ string, logGroups []*protocol.LogGroup) error {
	for _, logGroup := range logGroups {
		group, err := f.normalizeLegacyLogGroup(logGroup)
		if err != nil {
			f.recordDrop(err)
			continue
		}
		if group == nil {
			continue
		}
		if err := f.FlusherKafka.Export([]*models.PipelineGroupEvents{group}, nil); err != nil {
			return err
		}
	}
	return nil
}

// Export serializes V2 log events to the fixed ULAS JSON schema before sending them to Kafka.
func (f *FlusherUlasNormalizationKafka) Export(groups []*models.PipelineGroupEvents, _ pipeline.PipelineContext) error {
	for _, group := range groups {
		normalized, err := f.normalizeGroupForExport(group)
		if err != nil {
			f.recordDrop(err)
			continue
		}
		if normalized == nil {
			continue
		}
		if err := f.FlusherKafka.Export([]*models.PipelineGroupEvents{normalized}, nil); err != nil {
			return err
		}
	}
	return nil
}

func (f *FlusherUlasNormalizationKafka) initTimeLocation() error {
	if strings.TrimSpace(f.TimeZone) == "" {
		f.TimeZone = defaultTimeZone
	}
	if strings.TrimSpace(f.TimeFormat) == "" {
		f.TimeFormat = defaultTimeFormat
	}
	location, err := time.LoadLocation(f.TimeZone)
	if err != nil {
		return fmt.Errorf("load TimeZone %q: %w", f.TimeZone, err)
	}
	f.location = location
	return nil
}

func (f *FlusherUlasNormalizationKafka) normalizeGroupForExport(group *models.PipelineGroupEvents) (*models.PipelineGroupEvents, error) {
	if group == nil || len(group.Events) == 0 {
		return nil, nil
	}
	events := make([]models.PipelineEvent, 0, len(group.Events))
	for _, event := range group.Events {
		record, err := f.formatPipelineEvent(event, group.Group)
		if err != nil {
			f.recordDrop(err)
			continue
		}
		events = append(events, models.NewByteArray(record))
	}
	if len(events) == 0 {
		return nil, nil
	}
	return &models.PipelineGroupEvents{Group: group.Group, Events: events}, nil
}

func (f *FlusherUlasNormalizationKafka) normalizeLegacyLogGroup(logGroup *protocol.LogGroup) (*models.PipelineGroupEvents, error) {
	if logGroup == nil || len(logGroup.Logs) == 0 {
		return nil, nil
	}
	tags := make(map[string]string, len(logGroup.LogTags))
	for _, tag := range logGroup.LogTags {
		if tag != nil {
			addLegacyTag(tags, tag.Key, tag.Value)
		}
	}
	if tags["host.ip"] == "" && logGroup.Source != "" {
		tags["host.ip"] = logGroup.Source
	}
	events := make([]models.PipelineEvent, 0, len(logGroup.Logs))
	for _, log := range logGroup.Logs {
		if log == nil {
			f.recordDrop(fmt.Errorf("missing required ULAS fields: content"))
			continue
		}
		contents := make(map[string]string, len(log.Contents))
		logTags := copyStringMap(tags)
		for _, content := range log.Contents {
			if content == nil {
				continue
			}
			if strings.HasPrefix(content.Key, "__tag__:") {
				addLegacyTag(logTags, strings.TrimPrefix(content.Key, "__tag__:"), content.Value)
				continue
			}
			contents[content.Key] = content.Value
		}
		if log.Time == 0 {
			f.recordDrop(fmt.Errorf("missing required ULAS fields: pollTime"))
			continue
		}
		record, err := f.formatRecord(contents, logTags, time.Unix(int64(log.Time), 0))
		if err != nil {
			f.recordDrop(err)
			continue
		}
		events = append(events, models.NewByteArray(record))
	}
	if len(events) == 0 {
		return nil, nil
	}
	return &models.PipelineGroupEvents{
		Group:  models.NewGroup(models.NewMetadata(), models.NewTagsWithMap(tags)),
		Events: events,
	}, nil
}

func (f *FlusherUlasNormalizationKafka) formatPipelineEvent(event models.PipelineEvent, group *models.GroupInfo) ([]byte, error) {
	log, ok := event.(*models.Log)
	if !ok {
		return nil, fmt.Errorf("unsupported ULAS normalization event type: %T", event)
	}
	contents := make(map[string]string, log.GetIndices().Len())
	for key, value := range log.GetIndices().Iterator() {
		contents[key] = fmt.Sprint(value)
	}
	tags := make(map[string]string, log.GetTags().Len())
	for key, value := range log.GetTags().Iterator() {
		tags[key] = value
	}
	if group != nil {
		for key, value := range group.GetTags().Iterator() {
			tags[key] = value
		}
	}
	contents[models.ContentKey] = string(log.GetBody())
	if log.GetTimestamp() == 0 {
		return nil, fmt.Errorf("missing required ULAS fields: pollTime")
	}
	return f.formatRecord(contents, tags, time.Unix(0, int64(log.GetTimestamp())))
}

func (f *FlusherUlasNormalizationKafka) formatRecord(contents, tags map[string]string, timestamp time.Time) ([]byte, error) {
	record := normalizationRecord{
		Content:        contents[models.ContentKey],
		HostIP:         tags["host.ip"],
		HostName:       tags["host.name"],
		LogFilePath:    tags["log.file.path"],
		DatasourceID:   contents["datasourceId"],
		DatasourceName: contents["datasourceName"],
		TemplateID:     contents["templateId"],
	}
	missing := missingRequiredFields(record)
	record.PollTime = timestamp.In(f.location).Format(f.TimeFormat)
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required ULAS fields: %s", strings.Join(missing, ","))
	}

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(record); err != nil {
		return nil, fmt.Errorf("marshal ULAS normalization record: %w", err)
	}
	return bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), nil
}

func missingRequiredFields(record normalizationRecord) []string {
	fields := []struct {
		name  string
		value string
	}{
		{"content", record.Content},
		{"hostIp", record.HostIP},
		{"hostName", record.HostName},
		{"logFilePath", record.LogFilePath},
		{"datasourceId", record.DatasourceID},
		{"datasourceName", record.DatasourceName},
		{"templateId", record.TemplateID},
	}
	missing := make([]string, 0, len(fields))
	for _, field := range fields {
		if strings.TrimSpace(field.value) == "" {
			missing = append(missing, field.name)
		}
	}
	return missing
}

func (f *FlusherUlasNormalizationKafka) recordDrop(err error) {
	count := f.droppedRecords.Add(1)
	if f.context != nil {
		logger.Warning(f.context.GetRuntimeContext(), selfmonitor.FlusherFlushAlarm,
			"drop ULAS normalization record", "count", count, "error", err)
	}
}

func copyStringMap(source map[string]string) map[string]string {
	copy := make(map[string]string, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func addLegacyTag(tags map[string]string, key, value string) {
	switch key {
	case "__path__":
		tags["log.file.path"] = value
	case "__hostname__":
		tags["host.name"] = value
	default:
		tags[key] = value
	}
}

func init() {
	pipeline.Flushers[pluginType] = func() pipeline.Flusher {
		return NewFlusherUlasNormalizationKafka()
	}
}
