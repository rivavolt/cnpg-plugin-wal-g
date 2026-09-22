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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// partialSuffix names the sibling a WAL file is fetched into before it is moved into place.
const partialSuffix = ".walg-partial"

// fetchIntoPlace runs fetch against a sibling of dest and moves the result into dest only once
// fetch has succeeded and the bytes are on disk, so dest is either absent or complete.
//
// wal-g creates its destination before the download starts and leaves whatever it has written
// when it is interrupted, and pg_rewind --restore-target-wal hands the real pg_wal/<segment>
// path as the destination rather than RECOVERYXLOG. A fetch killed part way (a cancelled call, a
// killed pod, a stalled node) would otherwise leave a truncated segment there that every later
// rewind reads as the real one and fails on. The sibling sits in the same directory, so the
// rename is atomic and wal-g's prefetch directory, derived from the destination's directory, is
// the same.
func fetchIntoPlace(dest string, fetch func(target string) error) error {
	partial := dest + partialSuffix
	if err := os.Remove(partial); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("while removing a stale partial fetch: %w", err)
	}
	if err := fetch(partial); err != nil {
		_ = os.Remove(partial)
		return err
	}
	if err := syncPath(partial); err != nil {
		_ = os.Remove(partial)
		return fmt.Errorf("while syncing the fetched WAL: %w", err)
	}
	if err := os.Rename(partial, dest); err != nil {
		_ = os.Remove(partial)
		return fmt.Errorf("while moving the fetched WAL into place: %w", err)
	}
	if err := syncPath(filepath.Dir(dest)); err != nil {
		return fmt.Errorf("while syncing the WAL directory: %w", err)
	}
	return nil
}

func syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}
