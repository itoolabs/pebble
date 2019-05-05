// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
)

type test struct {
	// The list of ops to execute. The ops refer to slots in the batches, iters,
	// and snapshots slices.
	ops []op
	idx int
	// The DB the test is run on.
	db   *pebble.DB
	opts *pebble.Options
	// The slots for the batches, iterators, and snapshots. These are read and
	// written by the ops to pass state from one op to another.
	batches   []*pebble.Batch
	iters     []*pebble.Iterator
	snapshots []*pebble.Snapshot
}

func newTest(ops []op) *test {
	return &test{
		ops: ops,
	}
}

func (t *test) init(h *history, dir string, opts *pebble.Options) error {
	t.opts = opts.EnsureDefaults()

	db, err := pebble.Open(dir, t.opts)
	if err != nil {
		return err
	}
	h.Recordf("db.Open() // %v\n", err)

	t.db = db
	return nil
}

func (t *test) step(h *history) bool {
	if t.idx >= len(t.ops) {
		return false
	}
	t.ops[t.idx].run(t, h)
	t.idx++
	return true
}

func (t *test) finish(h *history) {
	db := t.db
	t.db = nil
	h.Recordf("db.Close() // %v\n", db.Close())
}

func (t *test) setBatch(id objID, b *pebble.Batch) {
	if id.tag() != batchTag {
		panic(fmt.Sprintf("invalid batch ID: %s", id))
	}
	t.batches[id.slot()] = b
}

func (t *test) setIter(id objID, i *pebble.Iterator) {
	if id.tag() != iterTag {
		panic(fmt.Sprintf("invalid iter ID: %s", id))
	}
	t.iters[id.slot()] = i
}

func (t *test) setSnapshot(id objID, s *pebble.Snapshot) {
	if id.tag() != snapTag {
		panic(fmt.Sprintf("invalid snapshot ID: %s", id))
	}
	t.snapshots[id.slot()] = s
}

func (t *test) clearObj(id objID) {
	switch id.tag() {
	case dbTag:
		panic("cannot clear DB ID")
	case batchTag:
		t.batches[id.slot()] = nil
	case iterTag:
		t.iters[id.slot()] = nil
	case snapTag:
		t.snapshots[id.slot()] = nil
	}
}

func (t *test) getBatch(id objID) *pebble.Batch {
	if id.tag() != batchTag {
		panic(fmt.Sprintf("invalid batch ID: %s", id))
	}
	return t.batches[id.slot()]
}

func (t *test) getCloser(id objID) io.Closer {
	switch id.tag() {
	case batchTag:
		return t.batches[id.slot()]
	case iterTag:
		return t.iters[id.slot()]
	case snapTag:
		return t.snapshots[id.slot()]
	}
	panic(fmt.Sprintf("cannot close ID: %s", id))
}

func (t *test) getIter(id objID) *pebble.Iterator {
	if id.tag() != iterTag {
		panic(fmt.Sprintf("invalid iter ID: %s", id))
	}
	return t.iters[id.slot()]
}

func (t *test) getReader(id objID) pebble.Reader {
	switch id.tag() {
	case dbTag:
		return t.db
	case batchTag:
		return t.batches[id.slot()]
	case snapTag:
		return t.snapshots[id.slot()]
	}
	panic(fmt.Sprintf("invalid reader ID: %s", id))
}

func (t *test) getSnapshot(id objID) *pebble.Snapshot {
	if id.tag() != snapTag {
		panic(fmt.Sprintf("invalid snapshot ID: %s", id))
	}
	return t.snapshots[id.slot()]
}

func (t *test) getWriter(id objID) pebble.Writer {
	switch id.tag() {
	case dbTag:
		return t.db
	case batchTag:
		return t.batches[id.slot()]
	}
	panic(fmt.Sprintf("invalid writer ID: %s", id))
}
