// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"log"
	"os"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/tool"
	"github.com/spf13/cobra"
)

var (
	cacheSize       int64
	concurrency     int
	disableWAL      bool
	duration        time.Duration
	engineType      string
	maxOpsPerSec    string
	verbose         bool
	waitCompactions bool
	wipe            bool
)

func main() {
	log.SetFlags(0)

	cobra.EnableCommandSorting = false

	benchCmd := &cobra.Command{
		Use:   "bench",
		Short: "benchmarks",
	}
	benchCmd.AddCommand(
		scanCmd,
		syncCmd,
		ycsbCmd,
	)

	rootCmd := &cobra.Command{
		Use:   "pebble [command] (flags)",
		Short: "pebble benchmarking/introspection tool",
	}
	rootCmd.AddCommand(benchCmd)

	t := tool.New()
	t.RegisterComparer(mvccComparer)
	t.RegisterMerger(func() *base.Merger {
		// TODO(peter): This isn't the actual cockroach_merge_operator, but a
		// placeholder so we can examine cockroach generated sstables.
		var m base.Merger
		m = *base.DefaultMerger
		m.Name = "cockroach_merge_operator"
		return &m
	}())
	rootCmd.AddCommand(t.Commands...)

	for _, cmd := range []*cobra.Command{scanCmd, syncCmd, ycsbCmd} {
		cmd.Flags().Int64Var(
			&cacheSize, "cache", 1<<30, "cache size")
		cmd.Flags().IntVarP(
			&concurrency, "concurrency", "c", 1, "number of concurrent workers")
		cmd.Flags().BoolVar(
			&disableWAL, "disable-wal", false, "disable the WAL (voiding persistence guarantees)")
		cmd.Flags().DurationVarP(
			&duration, "duration", "d", 10*time.Second, "the duration to run (0, run forever)")
		cmd.Flags().StringVarP(
			&engineType, "engine", "e", "pebble", "engine type (pebble [default], badger, boltdb, rocksdb)")
		cmd.Flags().StringVarP(
			&maxOpsPerSec, "rate", "m", "1000000", "max ops per second [{zipf,uniform}:]min[-max][/period (sec)]")
		cmd.Flags().BoolVarP(
			&verbose, "verbose", "v", false, "enable verbose event logging")
		cmd.Flags().BoolVar(
			&waitCompactions, "wait-compactions", false,
			"wait for background compactions to complete after load stops")
		cmd.Flags().BoolVarP(
			&wipe, "wipe", "w", false, "wipe the database before starting")
	}

	if err := rootCmd.Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
