/*
Copyright 2025 YANDEX LLC.

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

package instance

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func assertAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s should not exist, stat err=%v", path, err)
	}
}

func TestFetchIntoPlaceSuccess(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "000000010000000000000001")
	err := fetchIntoPlace(dest, func(target string) error {
		return os.WriteFile(target, []byte("segment"), 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "segment" {
		t.Fatalf("dest=%q err=%v", got, err)
	}
	assertAbsent(t, dest+partialSuffix)
}

func TestFetchIntoPlaceFailedFetchLeavesNothing(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "000000010000000000000001")
	err := fetchIntoPlace(dest, func(target string) error {
		_ = os.WriteFile(target, []byte("par"), 0o600)
		return errors.New("download broke")
	})
	if err == nil {
		t.Fatal("expected the fetch error")
	}
	assertAbsent(t, dest)
	assertAbsent(t, dest+partialSuffix)
}

func TestFetchIntoPlaceRemovesStalePartial(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "000000010000000000000001")
	if err := os.WriteFile(dest+partialSuffix, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// wal-g opens its destination O_EXCL, so a leftover partial would make every retry fail.
	err := fetchIntoPlace(dest, func(target string) error {
		f, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		_, err = f.WriteString("segment")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(dest); string(got) != "segment" {
		t.Fatalf("dest=%q", got)
	}
}

// A fetch killed mid-way, the way a cancelled gRPC call kills wal-g: the child has created its
// target and written part of it when the context ends.
func TestFetchIntoPlaceKilledFetchLeavesNothing(t *testing.T) {
	for _, after := range []time.Duration{0, 20 * time.Millisecond, 200 * time.Millisecond} {
		dest := filepath.Join(t.TempDir(), "000000010000000000000001")
		ctx, cancel := context.WithTimeout(context.Background(), after+50*time.Millisecond)
		err := fetchIntoPlace(dest, func(target string) error {
			return exec.CommandContext(ctx, "sh", "-c", `: > "$1"; sleep 0.02; printf part >> "$1"; sleep 5`, "sh", target).Run()
		})
		cancel()
		if err == nil {
			t.Fatalf("after %v: expected the kill to fail the fetch", after)
		}
		assertAbsent(t, dest)
		assertAbsent(t, dest+partialSuffix)
	}
}

func TestRestoreEnvKeepsPrefetchForRecoveryOnly(t *testing.T) {
	for dest, want := range map[string]string{
		"pg_wal/RECOVERYXLOG":             "",
		"/pgdata/pg_wal/RECOVERYHISTORY":  "",
		"pg_wal/000000010000000000000003": "1",
		"/pgdata/pg_wal/00000002.history": "1",
	} {
		got := restoreEnv(dest, map[string]string{"WALG_DOWNLOAD_CONCURRENCY": ""})["WALG_DOWNLOAD_CONCURRENCY"]
		if got != want {
			t.Errorf("%s: WALG_DOWNLOAD_CONCURRENCY = %q, want %q", dest, got, want)
		}
	}
}
