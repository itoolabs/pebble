// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package batch

import "github.com/cockroachdb/pebble/internal/base"

// Sort is a hook for constructing iterators over the point and range mutations
// contained in a batch in sorted order. It is intended for testing use only.
var Sort func(interface{}) (base.InternalIterator, base.InternalIterator)
