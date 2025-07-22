/*
Copyright 2025 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package logger exposes a singleton zap logger for adapter client.
package logger

import (
	"bytes"
	"fmt"
	"os"

	"cloud.google.com/go/spanner/adapter/apiv1/adapterpb"

	"github.com/datastax/go-cassandra-native-protocol/frame"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	zapLog *zap.Logger
	codec  = frame.NewCodec()
)

func SetupGlobalLogger(level string) error {
	var config zap.Config

	if os.Getenv("ADAPTER_CLI_ENV") == "dev" {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}

	logLevel := zapcore.InfoLevel
	if level != "" {
		err := logLevel.Set(level)
		if err != nil {
			return fmt.Errorf("invalid log level '%s': %w", level, err)
		}
	}
	config.Level.SetLevel(logLevel)

	var err error
	zapLog, err = config.Build(zap.AddCallerSkip(1))
	if err != nil {
		return fmt.Errorf("failed to build global logger: %w", err)
	}
	zapLog = zapLog.Named("go-spanner-cassandra")

	return nil
}

func Info(message string, fields ...zap.Field) {
	zapLog.Info(message, fields...)
}

func Debug(message string, fields ...zap.Field) {
	zapLog.Debug(message, fields...)
}

func Error(message string, fields ...zap.Field) {
	zapLog.Error(message, fields...)
}

func Fatal(message string, fields ...zap.Field) {
	zapLog.Fatal(message, fields...)
}

func DumpRequest(req *adapterpb.AdaptMessageRequest) error {
	frame, err := codec.DecodeFrame(bytes.NewBuffer(req.Payload))
	if err != nil {
		Debug("Error dumping request,", zap.Error(err))
		return err
	}
	zapLog.Debug(
		"Sent AdaptMessageRequest: ",
		zap.String("decoded frame", frame.Body.String()),
	)
	return nil
}

func DumpResponse(resp *adapterpb.AdaptMessageResponse) error {
	frame, err := codec.DecodeFrame(bytes.NewBuffer(resp.Payload))
	if err != nil {
		Debug("Error dumping response,", zap.Error(err))
		return err
	}
	zapLog.Debug(
		"Received AdaptMessageResponse: ",
		zap.String("decoded frame", frame.Body.String()),
	)
	return nil
}
