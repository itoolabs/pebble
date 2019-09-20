// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/internal/randvar"
	"golang.org/x/exp/rand"
)

type generator struct {
	rng *rand.Rand

	init *initOp
	ops  []op

	// keys that have been set in the DB. Used to reuse already generated keys
	// during random key selection.
	keys [][]byte

	// Unordered sets of object IDs for live objects. Used to randomly select on
	// object when generating an operation.
	liveBatches   objIDSlice
	liveIters     objIDSlice
	liveReaders   objIDSlice
	liveSnapshots objIDSlice
	liveWriters   objIDSlice

	maxLiveBatches   int
	maxLiveIters     int
	maxLiveReaders   int
	maxLiveSnapshots int
	maxLiveWriters   int

	// batchID -> batch iters: used to keep track of the open iterators on an
	// indexed batch. The iter set value will also be indexed by the readers map.
	batches map[objID]objIDSet
	// iterID -> reader iters: used to keep track of all of the open
	// iterators. The iter set value will also be indexed by either the batches
	// or snapshots maps.
	iters map[objID]objIDSet
	// readerID -> reader iters: used to keep track of the open iterators on a
	// reader. The iter set value will also be inexed by either the batches or
	// snapshots maps.
	readers map[objID]objIDSet
	// snapshotID -> snapshot iters: used to keep track of the open iterators on
	// a snapshot. The iter set value will also be indexed by the readers map.
	snapshots map[objID]objIDSet
}

func newGenerator() *generator {
	g := &generator{
		rng:         rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
		init:        &initOp{},
		liveReaders: objIDSlice{makeObjID(dbTag, 0)},
		liveWriters: objIDSlice{makeObjID(dbTag, 0)},
		batches:     make(map[objID]objIDSet),
		iters:       make(map[objID]objIDSet),
		readers:     make(map[objID]objIDSet),
		snapshots:   make(map[objID]objIDSet),
	}
	g.ops = append(g.ops, g.init)
	return g
}

func generate(min, max int, cfg config) []op {
	g := newGenerator()

	generators := []func(){
		batchAbort:        g.batchAbort,
		batchCommit:       g.batchCommit,
		iterClose:         g.iterClose,
		iterFirst:         g.iterFirst,
		iterLast:          g.iterLast,
		iterNext:          g.iterNext,
		iterPrev:          g.iterPrev,
		iterSeekGE:        g.iterSeekGE,
		iterSeekLT:        g.iterSeekLT,
		iterSeekPrefixGE:  g.iterSeekPrefixGE,
		iterSetBounds:     g.iterSetBounds,
		newBatch:          g.newBatch,
		newIndexedBatch:   g.newIndexedBatch,
		newIter:           g.newIter,
		newSnapshot:       g.newSnapshot,
		readerGet:         g.readerGet,
		snapshotClose:     g.snapshotClose,
		writerApply:       g.writerApply,
		writerDelete:      g.writerDelete,
		writerDeleteRange: g.writerDeleteRange,
		writerIngest:      g.writerIngest,
		writerMerge:       g.writerMerge,
		writerSet:         g.writerSet,
	}

	// TPCC-style deck of cards randomization. Every time the end of the deck is
	// reached, we shuffle the deck.
	deck := randvar.NewDeck(g.rng, cfg.ops...)

	count := min
	if max > min {
		count += g.rng.Intn(max - min)
	}
	for i := 0; i < count; i++ {
		generators[deck.Int()]()
	}

	for len(g.liveSnapshots) > 0 {
		g.snapshotClose()
	}
	for len(g.liveIters) > 0 {
		g.iterClose()
	}
	return g.ops
}

func (g *generator) add(op op) {
	if liveBatches := len(g.liveBatches); g.maxLiveBatches < liveBatches {
		g.maxLiveBatches = liveBatches
	}
	if liveIters := len(g.liveIters); g.maxLiveIters < liveIters {
		g.maxLiveIters = liveIters
	}
	if liveReaders := len(g.liveReaders); g.maxLiveReaders < liveReaders {
		g.maxLiveReaders = liveReaders
	}
	if liveSnapshots := len(g.liveSnapshots); g.maxLiveSnapshots < liveSnapshots {
		g.maxLiveSnapshots = liveSnapshots
	}
	if liveWriters := len(g.liveWriters); g.maxLiveWriters < liveWriters {
		g.maxLiveWriters = liveWriters
	}

	g.ops = append(g.ops, op)
}

// TODO(peter): make this configurable. See config.go.
func (g *generator) randKey(newKey float64) []byte {
	if n := len(g.keys); n > 0 && g.rng.Float64() > newKey {
		return g.keys[g.rng.Intn(n)]
	}
	key := g.randValue(4, 12)
	g.keys = append(g.keys, key)
	return key
}

// TODO(peter): make this configurable. See config.go.
func (g *generator) randValue(min, max int) []byte {
	const letters = "+.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	const lettersLen = uint64(len(letters))
	const lettersCharsPerRand = 10 // floor(log(math.MaxUint64)/log(lettersLen))

	n := min
	if max > min {
		n += g.rng.Intn(max - min)
	}

	buf := make([]byte, n)

	var r uint64
	var q int
	for i := 0; i < len(buf); i++ {
		if q == 0 {
			r = g.rng.Uint64()
			q = lettersCharsPerRand
		}
		buf[i] = letters[r%lettersLen]
		r = r / lettersLen
		q--
	}

	return buf
}

func (g *generator) newBatch() {
	batchID := makeObjID(batchTag, g.init.batchSlots)
	g.init.batchSlots++
	g.liveBatches = append(g.liveBatches, batchID)
	g.liveWriters = append(g.liveWriters, batchID)

	g.add(&newBatchOp{
		batchID: batchID,
	})
}

func (g *generator) newIndexedBatch() {
	batchID := makeObjID(batchTag, g.init.batchSlots)
	g.init.batchSlots++
	g.liveBatches = append(g.liveBatches, batchID)
	g.liveReaders = append(g.liveReaders, batchID)
	g.liveWriters = append(g.liveWriters, batchID)

	iters := make(objIDSet)
	g.batches[batchID] = iters
	g.readers[batchID] = iters

	g.add(&newIndexedBatchOp{
		batchID: batchID,
	})
}

func (g *generator) batchClose(batchID objID) {
	g.liveBatches.remove(batchID)
	iters := g.batches[batchID]
	delete(g.batches, batchID)

	if iters != nil {
		g.liveReaders.remove(batchID)
		delete(g.readers, batchID)
	}
	g.liveWriters.remove(batchID)
	for id := range iters {
		g.liveIters.remove(id)
		delete(g.iters, id)
		g.add(&closeOp{objID: id})
	}
}

func (g *generator) batchAbort() {
	if len(g.liveBatches) == 0 {
		return
	}

	batchID := g.liveBatches.rand(g.rng)
	g.batchClose(batchID)

	g.add(&closeOp{objID: batchID})
}

func (g *generator) batchCommit() {
	if len(g.liveBatches) == 0 {
		return
	}

	batchID := g.liveBatches.rand(g.rng)
	g.batchClose(batchID)

	g.add(&batchCommitOp{
		batchID: batchID,
	})
}

func (g *generator) newIter() {
	iterID := makeObjID(iterTag, g.init.iterSlots)
	g.init.iterSlots++
	g.liveIters = append(g.liveIters, iterID)

	readerID := g.liveReaders.rand(g.rng)
	if iters := g.readers[readerID]; iters != nil {
		iters[iterID] = struct{}{}
		g.iters[iterID] = iters
	}

	var lower, upper []byte
	if g.rng.Float64() <= 0.1 {
		lower = g.randKey(0.001)
	}
	if g.rng.Float64() <= 0.1 {
		upper = g.randKey(0.001)
	}
	if bytes.Compare(lower, upper) > 0 {
		lower, upper = upper, lower
	}

	g.add(&newIterOp{
		readerID: readerID,
		iterID:   iterID,
		lower:    lower,
		upper:    upper,
	})
}

func (g *generator) iterClose() {
	if len(g.liveIters) == 0 {
		return
	}

	iterID := g.liveIters.rand(g.rng)
	g.liveIters.remove(iterID)
	if readerIters, ok := g.iters[iterID]; ok {
		delete(g.iters, iterID)
		delete(readerIters, iterID)
	}

	g.add(&closeOp{objID: iterID})
}

func (g *generator) iterSetBounds() {
	if len(g.liveIters) == 0 {
		return
	}

	var lower, upper []byte
	if g.rng.Float64() <= 0.5 {
		lower = g.randKey(0.001)
	}
	if g.rng.Float64() <= 0.5 {
		upper = g.randKey(0.001)
	}
	if bytes.Compare(lower, upper) > 0 {
		lower, upper = upper, lower
	}
	g.add(&iterSetBoundsOp{
		iterID: g.liveIters.rand(g.rng),
		lower:  lower,
		upper:  upper,
	})
}

func (g *generator) iterSeekGE() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterSeekGEOp{
		iterID: g.liveIters.rand(g.rng),
		key:    g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) iterSeekPrefixGE() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterSeekPrefixGEOp{
		iterID: g.liveIters.rand(g.rng),
		key:    g.randKey(0), // 0% new keys
	})
}

func (g *generator) iterSeekLT() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterSeekLTOp{
		iterID: g.liveIters.rand(g.rng),
		key:    g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) iterFirst() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterFirstOp{
		iterID: g.liveIters.rand(g.rng),
	})
}

func (g *generator) iterLast() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterLastOp{
		iterID: g.liveIters.rand(g.rng),
	})
}

func (g *generator) iterNext() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterNextOp{
		iterID: g.liveIters.rand(g.rng),
	})
}

func (g *generator) iterPrev() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterPrevOp{
		iterID: g.liveIters.rand(g.rng),
	})
}

func (g *generator) readerGet() {
	if len(g.liveReaders) == 0 {
		return
	}

	g.add(&getOp{
		readerID: g.liveReaders.rand(g.rng),
		key:      g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) newSnapshot() {
	snapID := makeObjID(snapTag, g.init.snapshotSlots)
	g.init.snapshotSlots++
	g.liveSnapshots = append(g.liveSnapshots, snapID)
	g.liveReaders = append(g.liveReaders, snapID)

	iters := make(objIDSet)
	g.snapshots[snapID] = iters
	g.readers[snapID] = iters

	g.add(&newSnapshotOp{
		snapID: snapID,
	})
}

func (g *generator) snapshotClose() {
	if len(g.liveSnapshots) == 0 {
		return
	}

	snapID := g.liveSnapshots.rand(g.rng)
	g.liveSnapshots.remove(snapID)
	iters := g.snapshots[snapID]
	delete(g.snapshots, snapID)
	g.liveReaders.remove(snapID)
	delete(g.readers, snapID)

	for id := range iters {
		g.liveIters.remove(id)
		delete(g.iters, id)
		g.add(&closeOp{objID: id})
	}

	g.add(&closeOp{objID: snapID})
}

func (g *generator) writerApply() {
	if len(g.liveWriters) == 0 || len(g.liveBatches) == 0 {
		return
	}

	batchID := g.liveBatches.rand(g.rng)

	var writerID objID
	if len(g.liveWriters) > 1 {
		for {
			writerID = g.liveWriters.rand(g.rng)
			if writerID != batchID {
				break
			}
		}
	}

	g.batchClose(batchID)

	g.add(&applyOp{
		writerID: writerID,
		batchID:  batchID,
	})
}

func (g *generator) writerDelete() {
	if len(g.liveWriters) == 0 {
		return
	}

	g.add(&deleteOp{
		writerID: g.liveWriters.rand(g.rng),
		key:      g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) writerDeleteRange() {
	if len(g.liveWriters) == 0 {
		return
	}

	start := g.randKey(0.001)
	end := g.randKey(0.001)
	if bytes.Compare(start, end) > 0 {
		start, end = end, start
	}

	g.add(&deleteRangeOp{
		writerID: g.liveWriters.rand(g.rng),
		start:    start,
		end:      end,
	})
}

func (g *generator) writerIngest() {
	if len(g.liveBatches) == 0 {
		return
	}

	// TODO(peter): randomly choose N batches to ingest.
	batchID := g.liveBatches.rand(g.rng)
	g.batchClose(batchID)

	g.add(&ingestOp{
		batchIDs: []objID{batchID},
	})
}

func (g *generator) writerMerge() {
	if len(g.liveWriters) == 0 {
		return
	}

	g.add(&mergeOp{
		writerID: g.liveWriters.rand(g.rng),
		key:      g.randKey(0.2), // 20% new keys
		value:    g.randValue(0, 20),
	})
}

func (g *generator) writerSet() {
	if len(g.liveWriters) == 0 {
		return
	}

	g.add(&setOp{
		writerID: g.liveWriters.rand(g.rng),
		key:      g.randKey(0.5), // 50% new keys
		value:    g.randValue(0, 20),
	})
}

func (g *generator) String() string {
	var buf bytes.Buffer
	for _, op := range g.ops {
		fmt.Fprintf(&buf, "%s\n", op)
	}
	fmt.Fprintf(&buf, "\n")
	fmt.Fprintf(&buf, "max-live-batches:   %d\n", g.maxLiveBatches)
	fmt.Fprintf(&buf, "max-live-iters:     %d\n", g.maxLiveIters)
	fmt.Fprintf(&buf, "max-live-readers:   %d\n", g.maxLiveReaders)
	fmt.Fprintf(&buf, "max-live-snapshots: %d\n", g.maxLiveSnapshots)
	fmt.Fprintf(&buf, "max-live-writers:   %d\n", g.maxLiveWriters)
	return buf.String()
}
