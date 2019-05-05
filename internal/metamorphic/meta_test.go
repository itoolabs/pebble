// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cache"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

func TestMeta(t *testing.T) {
	// TODO(peter): Run the test repeatedly with different options. In between
	// each step, randomly flush or compact. Verify the histories of each test
	// are identical. Options to randomize:
	//
	//   LevelOptions
	//     BlockRestartInterval
	//     BlockSize
	//     BlockSizeThreshold
	//     Compression
	//     FilterType
	//     IndexBlockSize
	//     TargetFileSize
	//   Options
	//     Cache size
	//     L0CompactionThreshold
	//     L0StopWritesThreshold
	//     LBaseMaxBytes
	//     MaxManifestFileSize
	//     MaxOpenFiles
	//     MemTableSize
	//     MemTableStopWritesThreshold
	//
	// In addition to random options, there should be a list of specific option
	// configurations, such as 0 size cache, 1-byte TargetFileSize, 1-byte
	// L1MaxBytes, etc. These extrema configurations will help exercise edge
	// cases.
	//
	// Verify the metamorphic test catchs various bugs:
	// - Instability of keys returned from range-del-iters and used by truncate
	// - Lack of support for lower/upper bound in flushableBatchIter
	//
	// Miscellaneous:
	// - Add support for different comparers. In particular, allow reverse
	//   comparers and a comparer which supports Comparer.Split (by splitting off
	//   a variable length suffix).
	// - Ingest and Apply can be randomly swapped leading to testing of
	//   interesting cases.
	// - DeleteRange can be used to replace Delete, stressing the DeleteRange
	//   implementation.
	ops := generate(5000, 10000, defaultConfig)
	m := newTest(ops)
	// TODO(peter): The history should probably be stored to a file on disk.
	h := newHistory(testing.Verbose())
	l := h.Logger()

	comparer := *pebble.DefaultComparer
	comparer.Split = func(a []byte) int {
		return len(a)
	}

	opts := &pebble.Options{
		Cache:         cache.New(1 << 30), // 1 GB
		Comparer:      &comparer,
		EventListener: base.MakeLoggingEventListener(l),
		Logger:        l,
		// FS:            vfs.Default,
		FS: vfs.NewMem(),
	}
	origBackgroundError := opts.EventListener.BackgroundError
	opts.EventListener.BackgroundError = func(err error) {
		origBackgroundError(err)
		os.Exit(1)
	}

	const dir = "tmp"
	os.RemoveAll(dir)

	if err := m.init(h, dir, opts); err != nil {
		t.Fatal(err)
	}
	for m.step(h) {
	}
	m.finish(h)
}
