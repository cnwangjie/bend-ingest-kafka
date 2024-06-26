package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	godatabend "github.com/datafuselabs/databend-go"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

type DatabendIngester interface {
	IngestData(messageBatch *message.MessagesBatch) error
	CreateRawTargetTable() error
}

type databendIngester struct {
	databendIngesterCfg *config.Config
	statsRecorder       *DatabendIngesterStatsRecorder
}

func NewDatabendIngester(cfg *config.Config) DatabendIngester {
	stats := NewDatabendIntesterStatsRecorder()
	return &databendIngester{
		databendIngesterCfg: cfg,
		statsRecorder:       stats,
	}
}

func (ig *databendIngester) reWriteTheJsonData(messagesBatch *message.MessagesBatch) ([]string, error) {
	batchJsonData := messagesBatch.Messages
	afterHandleJsonData := make([]string, 0, len(batchJsonData))

	for i := 0; i < len(batchJsonData); i++ {
		// re-write the json data into NDJson format, add the uuid, record_metadata and add_time fields
		recordMetadata := fmt.Sprintf("{\"topic\":\"%s\", \"partition\":\"%d\",\"offset\":\"%d\", \"key\":\"%s\", \"create_time\":\"%s\"}",
			ig.databendIngesterCfg.KafkaTopic,
			batchJsonData[i].Partition,
			batchJsonData[i].DataOffset,
			batchJsonData[i].Key,
			batchJsonData[i].CreateTime.Format(time.RFC3339Nano))
		// add the uuid, record_metadata and add_time fields
		d := fmt.Sprintf("{\"uuid\":\"%s\",\"record_metadata\":%s,\"add_time\":\"%s\",\"raw_data\":%s}",
			uuid.New().String(),
			recordMetadata,
			time.Now().Format(time.RFC3339Nano),
			batchJsonData[i].Data)
		afterHandleJsonData = append(afterHandleJsonData, d)
	}
	return afterHandleJsonData, nil
}

func (ig *databendIngester) IngestData(messageBatch *message.MessagesBatch) error {
	startTime := time.Now()
	if messageBatch == nil {
		return nil
	}
	batchJsonData := messageBatch.ExtractMessageData()

	if len(batchJsonData) == 0 {
		return nil
	}
	// handle batchJsonData, if isTransform is false, then the data is already in NDJson format
	// re-write the json data into NDJson format, add the uuid, record_metadata and add_time fields
	// then insert the data into the databend table
	if !ig.databendIngesterCfg.IsJsonTransform {
		var err error
		batchJsonData, err = ig.reWriteTheJsonData(messageBatch)
		if err != nil {
			return err
		}
	}

	fileName, bytesSize, err := ig.generateNDJsonFile(batchJsonData)
	if err != nil {
		return err
	}

	stage, err := ig.uploadToStage(fileName)
	if err != nil {
		return err
	}

	err = ig.copyInto(stage)
	if err != nil {
		return err
	}

	ig.statsRecorder.RecordMetric(bytesSize, len(batchJsonData))
	stats := ig.statsRecorder.Stats(time.Since(startTime))
	log.Printf("ingest %d rows (%f rows/s), %d bytes (%f bytes/s)", len(batchJsonData), stats.RowsPerSecondd, bytesSize, stats.BytesPerSecond)
	return nil
}

func (ig *databendIngester) generateNDJsonFile(batchJsonData []string) (string, int, error) {
	outputFile, err := ioutil.TempFile("/tmp", "databend-ingest-*.ndjson")
	if err != nil {
		return "", 0, err
	}
	defer outputFile.Close()

	// Create a buffered writer for the Ndjson file
	writer := bufio.NewWriter(outputFile)
	bytesSum := 0

	for _, data := range batchJsonData {
		n, err := writer.WriteString(data + "\n")
		if err != nil {
			return "", 0, err
		}
		bytesSum += n
	}
	// Flush any remaining data to the NDJson file
	err = writer.Flush()
	if err != nil {
		return "", 0, err
	}
	return outputFile.Name(), bytesSum, err
}

func (ig *databendIngester) uploadToStage(fileName string) (*godatabend.StageLocation, error) {
	defer func() {
		err := os.RemoveAll(fileName)
		if err != nil {
			logrus.Errorf("delete batch insert file failed: %v", err)
		}
	}()

	databendConfig, err := godatabend.ParseDSN(ig.databendIngesterCfg.DatabendDSN)
	if err != nil {
		return nil, err
	}
	apiClient := godatabend.NewAPIClientFromConfig(databendConfig)
	fi, err := os.Stat(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "get batch file size failed")
	}
	size := fi.Size()

	f, err := os.Open(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "open batch file failed")
	}
	defer f.Close()
	input := bufio.NewReader(f)
	stage := &godatabend.StageLocation{
		Name: "~",
		Path: fmt.Sprintf("batch/%d-%s", time.Now().Unix(), filepath.Base(fileName)),
	}

	return stage, apiClient.UploadToStage(context.Background(), stage, input, size)
}

func execute(db *sql.DB, sql string) error {
	_, err := db.Exec(sql)
	if err != nil {
		logrus.Errorf("exec '%s' failed, err: %v", sql, err)
		return err
	}
	return nil
}

func (ig *databendIngester) copyInto(stage *godatabend.StageLocation) error {
	copyIntoSQL := fmt.Sprintf("COPY INTO %s FROM %s FILE_FORMAT = (type = NDJSON missing_field_as = FIELD_DEFAULT COMPRESSION = AUTO) "+
		"PURGE = %v FORCE = %v DISABLE_VARIANT_CHECK = %v", ig.databendIngesterCfg.DatabendTable, stage.String(),
		ig.databendIngesterCfg.CopyPurge, ig.databendIngesterCfg.CopyForce, ig.databendIngesterCfg.DisableVariantCheck)
	db, err := sql.Open("databend", ig.databendIngesterCfg.DatabendDSN)
	if err != nil {
		logrus.Errorf("create db error: %v", err)
		return err
	}
	return execute(db, copyIntoSQL)
}

func (ig *databendIngester) CreateRawTargetTable() error {
	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (uuid String, raw_data json, record_metadata json, add_time timestamp) ", ig.databendIngesterCfg.DatabendTable)
	db, err := sql.Open("databend", ig.databendIngesterCfg.DatabendDSN)
	if err != nil {
		logrus.Errorf("create db error: %v", err)
		return err
	}
	return execute(db, createTableSQL)
}
