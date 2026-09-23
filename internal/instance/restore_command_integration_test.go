//go:build integration

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
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestRestoreCommand is the restore_command of hack/rewind-integration.sh rather than a test of its own: that scenario runs the compiled test binary as Postgres's restore_command, so every WAL file pg_rewind asks for goes through fetchIntoPlace and `wal-g wal-fetch` exactly as the plugin's Restore sends it. RESTORE_KILL_AFTER kills wal-g that long after it starts, the way a cancelled call or a killed pod does.
func TestRestoreCommand(t *testing.T) {
	source, dest := os.Getenv("RESTORE_SOURCE"), os.Getenv("RESTORE_DEST")
	if source == "" || dest == "" {
		t.Skip("run only as a restore_command")
	}
	ctx := context.Background()
	if after := os.Getenv("RESTORE_KILL_AFTER"); after != "" {
		d, err := time.ParseDuration(after)
		if err != nil {
			t.Fatal(err)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	err := fetchIntoPlace(dest, func(target string) error {
		c := exec.CommandContext(ctx, "wal-g", "wal-fetch", source, target)
		c.Stdout, c.Stderr = os.Stderr, os.Stderr
		return c.Run()
	})
	if err != nil {
		t.Fatal(err)
	}
}
