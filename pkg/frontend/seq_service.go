// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"golang.org/x/exp/constraints"
)

const (
	selectSeq        = "select * from `%s`.`%s`;"
	updateSeq        = "update '%s'.'%s' set last_seq_num = %d;"
	updateSeqSetTrue = "update '%s'.'%s' set is_called = true;"

	setEdge  = true
	NoLoop   = true
	NoChange = true
)

// tableId
type SeqKey uint64

// TODO: gc.

// ToFix:
// Begin;
// Create Sequence s1;
// select nextval(s1);
// Can't see.

type SeqGenerator struct {
	BgMu      sync.Mutex
	BgHandler BackgroundExec
	// Cache here will just be set by the system.
	// will support specify cache size when initialize the
	// sequence object in the future.
	CacheSize int64

	Ctx context.Context

	// Map to stored seq data.
	// Key: SeqKey
	// Value: SeqCache
	CachedSeq sync.Map

	// Using seq mutex to update SeqCache.
	// Key: SeqKey
	// Value: *sync.Mu
	SeqMutex sync.Map
	// Last updated ts to the seq object with this Bghandler.
	// tsMap map[SeqKey]types.Timestamp
}

type SeqCache struct {
	// All types stored as int64, even uint64.
	lsn   int64
	maxsn int64

	minv   int64
	maxv   int64
	startv int64

	incr     int64
	cycle    bool
	iscalled bool
	oid      types.T
}

// 1. The tableId must reference to a sequence object.(Caller's duty)
// 2. The object may be deleted and the cache still exists. Not really a big deal,
// still generating unique value.
func (sg *SeqGenerator) GetNextVal(tableId uint64, tableName, dbName string, oid types.T) (string, error) {
	key := SeqKey(tableId)
	// Try getting from seq cache.
	if sc, exists := sg.CachedSeq.Load(key); exists {
		// Exists.
		mu, exists := sg.SeqMutex.Load(key)
		if !exists {
			return "", moerr.NewInternalError(sg.Ctx, "Got seq %s.%s cached but no mutext", dbName, tableName)
		}
		mtx := mu.(*sync.Mutex)

		// sc may be changed in this line of code.
		mtx.Lock()
		defer mtx.Unlock()
		// sc may be dropped here.
		seqCache := sc.(*SeqCache)
		// Advance and return the advanced lsn.
		s, err, runOut := sg.advanceCache(seqCache, oid)
		if err != nil {
			return "", err
		}
		// TODO: If the cache lefts only 1/4.Starting a goroutine to extend the cache.
		// Just access the sequence object when the cache runs out now.
		if runOut {
			sg.BgMu.Lock()
			nsc, err := sg.advanceSequenceObject(tableName, dbName)
			if err != nil {
				sg.BgMu.Unlock()
				return "", err
			}
			updateCache(seqCache, nsc)
			sg.BgMu.Unlock()
		} else {
			return s, nil
		}

		if s, err, _ = sg.advanceCache(seqCache, oid); err != nil {
			return "", err
		}

		return s, nil
	}

	// Not cached.
	sg.BgMu.Lock()
	sc, err := sg.advanceSequenceObject(tableName, dbName)
	sg.BgMu.Unlock()
	if err != nil {
		return "", err
	}

	// Init the cache.
	sc.oid = oid
	sg.SeqMutex.Store(key, &sync.Mutex{})
	sg.CachedSeq.Store(key, sc)

	// Get the value again.
	return sg.GetNextVal(tableId, tableName, dbName, oid)
}

func (sg *SeqGenerator) advanceCache(sc *SeqCache, oid types.T) (string, error, bool) {
	if sc.oid != oid {
		return "", moerr.NewInternalError(sg.Ctx, "Sequence type doesn't match"), false
	}

	res := fmt.Sprintf("%d", sc.lsn)
	if !sc.iscalled {
		sc.iscalled = true
		return res, nil, false
	}

	if oid == types.T_uint64 {

	}

	// Descending
	if sc.incr < 0 {
		updated := sc.lsn + sc.incr
		if updated > sc.maxsn && updated < sc.lsn {
			sc.lsn = updated
			return res, nil, false
		}

		if sc.maxsn > sc.lsn {

		}
	} else {

	}

	return "", nil, false
}

func updateCache(sc, nsc *SeqCache) {
	sc.cycle = nsc.cycle
	sc.iscalled = nsc.iscalled
	sc.lsn = nsc.lsn
	sc.maxsn = nsc.maxsn
}

func (sg *SeqGenerator) advanceSequenceObject(tblName, dbName string) (*SeqCache, error) {
	var err error
	var erArray []ExecResult
	var values []interface{}
	var seqCache *SeqCache
	var noloop bool
	tried := 0
loop:
	if tried == 100 {
		return nil, err
	}
	tried++

	if err = sg.BgHandler.Exec(sg.Ctx, "begin;"); err != nil {
		goto handleFailed
	}

	sg.BgHandler.ClearExecResultSet()
	// get the sequence data.
	if err = sg.BgHandler.Exec(sg.Ctx, fmt.Sprintf(selectSeq, dbName, tblName)); err != nil {
		goto handleFailed
	}
	erArray, err = getResultSet(sg.Ctx, sg.BgHandler)
	if err != nil {
		goto handleFailed
	}

	if len(erArray) == 0 {
		err = moerr.NewInternalError(sg.Ctx, "Wrong sequence data")
		goto handleFailed
	}
	values = erArray[0].(*MysqlResultSet).Data[0]
	// Here update the object data.
	if seqCache, err, noloop = sg.updateAndGetCacheFromObj(values, dbName, tblName); err != nil {
		if !noloop {
			goto handleFailed
		}
		// Encounter error not related to the txn.
		return nil, err
	}

	// Success.
	if err = sg.BgHandler.Exec(sg.Ctx, "commit;"); err != nil {
		goto handleFailed
	}

	// Finaly return the newcache.
	return seqCache, nil
handleFailed:
	//ROLLBACK the transaction
	err = sg.BgHandler.Exec(sg.Ctx, "rollback;")
	goto loop
}

// Incr is too big but can cycle.
// Incr is too big but can not cycle.
func (sg *SeqGenerator) updateAndGetCacheFromObj(values []interface{}, dbName, tblName string) (*SeqCache, error, bool) {
	var err error
	switch values[0].(type) {
	case int16:
		// Get values store in sequence table.
		lsn, minv, maxv, startv, incrv, cycle, isCalled := values[0].(int16), values[1].(int16), values[2].(int16),
			values[3].(int16), values[4].(int64), values[5].(bool), values[6].(bool)
		// When iscalled is not set, set it and do not advance sequence number.
		if !isCalled {
			if err = sg.BgHandler.Exec(sg.Ctx, fmt.Sprintf(updateSeqSetTrue, dbName, tblName)); err != nil {
				return nil, err, !NoLoop
			}
		}
		// When incr is over the range of this datatype.
		if incrv > math.MaxInt16 || incrv < math.MinInt16 {
			if cycle {
				v, err := advanceSeq(lsn, minv, maxv, int16(incrv), cycle, incrv < 0, setEdge, dbName, tblName, sg)
				if err != nil {
					return nil, err, !NoLoop
				}
				return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
			} else {
				// This case may need optimization.Every time acquiring the bghandler and just no effect.
				return nil, moerr.NewInternalError(sg.Ctx, "Reached maximum value of sequence %s", tblName), NoLoop
			}
		}
		// Tranforming incrv to this datatype and make it positive for generic use.
		v, err := advanceSeq(lsn, minv, maxv, makePosIncr[int16](incrv), cycle, incrv < 0, !setEdge, dbName, tblName, sg)
		if err != nil {
			return nil, err, !NoLoop
		}
		return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
	case int32:
		lsn, minv, maxv, startv, incrv, cycle, isCalled := values[0].(int32), values[1].(int32), values[2].(int32),
			values[3].(int32), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			if err = sg.BgHandler.Exec(sg.Ctx, fmt.Sprintf(updateSeqSetTrue, dbName, tblName)); err != nil {
				return nil, err, !NoLoop
			}
		}
		if incrv > math.MaxInt64 || incrv < math.MinInt64 {
			if cycle {
				v, err := advanceSeq(lsn, minv, maxv, int32(incrv), cycle, incrv < 0, setEdge, dbName, tblName, sg)
				if err != nil {
					return nil, err, !NoLoop
				}
				return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
			} else {
				return nil, moerr.NewInternalError(sg.Ctx, "Reached maximum value of sequence %s", tblName), NoLoop
			}
		}
		v, err := advanceSeq(lsn, minv, maxv, makePosIncr[int32](incrv), cycle, incrv < 0, !setEdge, dbName, tblName, sg)
		if err != nil {
			return nil, err, !NoLoop
		}
		return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
	case int64:
		lsn, minv, maxv, startv, incrv, cycle, isCalled := values[0].(int64), values[1].(int64), values[2].(int64),
			values[3].(int64), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			if err = sg.BgHandler.Exec(sg.Ctx, fmt.Sprintf(updateSeqSetTrue, dbName, tblName)); err != nil {
				return nil, err, !NoLoop
			}
		}
		v, err := advanceSeq(lsn, minv, maxv, makePosIncr[int64](incrv), cycle, incrv < 0, !setEdge, dbName, tblName, sg)
		if err != nil {
			return nil, err, !NoLoop
		}
		return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
	case uint16:
		lsn, minv, maxv, startv, incrv, cycle, isCalled := values[0].(uint16), values[1].(uint16), values[2].(uint16),
			values[3].(uint16), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			if err = sg.BgHandler.Exec(sg.Ctx, fmt.Sprintf(updateSeqSetTrue, dbName, tblName)); err != nil {
				return nil, err, !NoLoop
			}
		}
		if incrv > math.MaxUint16 || -incrv > math.MaxUint16 {
			if cycle {
				v, err := advanceSeq(lsn, minv, maxv, uint16(incrv), cycle, incrv < 0, setEdge, dbName, tblName, sg)
				if err != nil {
					return nil, err, !NoLoop
				}
				return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
			} else {
				return nil, moerr.NewInternalError(sg.Ctx, "Reached maximum value of sequence %s", tblName), NoLoop
			}
		}
		v, err := advanceSeq(lsn, minv, maxv, makePosIncr[uint16](incrv), cycle, incrv < 0, !setEdge, dbName, tblName, sg)
		if err != nil {
			return nil, err, !NoLoop
		}
		return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
	case uint32:
		lsn, minv, maxv, startv, incrv, cycle, isCalled := values[0].(uint32), values[1].(uint32), values[2].(uint32),
			values[3].(uint32), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			if err = sg.BgHandler.Exec(sg.Ctx, fmt.Sprintf(updateSeqSetTrue, dbName, tblName)); err != nil {
				return nil, err, !NoLoop
			}
		}
		if incrv > math.MaxUint32 || -incrv > math.MaxUint32 {
			if cycle {
				v, err := advanceSeq(lsn, minv, maxv, uint32(incrv), cycle, incrv < 0, setEdge, dbName, tblName, sg)
				if err != nil {
					return nil, err, !NoLoop
				}
				return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
			} else {
				return nil, moerr.NewInternalError(sg.Ctx, "Reached maximum value of sequence %s", tblName), NoLoop
			}
		}
		v, err := advanceSeq(lsn, minv, maxv, makePosIncr[uint32](incrv), cycle, incrv < 0, !setEdge, dbName, tblName, sg)
		if err != nil {
			return nil, err, !NoLoop
		}
		return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
	case uint64:
		lsn, minv, maxv, startv, incrv, cycle, isCalled := values[0].(uint64), values[1].(uint64), values[2].(uint64),
			values[3].(uint64), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			if err = sg.BgHandler.Exec(sg.Ctx, fmt.Sprintf(updateSeqSetTrue, dbName, tblName)); err != nil {
				return nil, err, NoLoop
			}
		}
		v, err := advanceSeq(lsn, minv, maxv, makePosIncr[uint64](incrv), cycle, incrv < 0, !setEdge, dbName, tblName, sg)
		if err != nil {
			return nil, err, !NoLoop
		}
		return makeSeqCache(v, lsn, minv, maxv, startv, incrv, cycle, isCalled), nil, NoLoop
	}

	return nil, moerr.NewInternalError(sg.Ctx, "Wrong types of sequence number or failed to read the sequence table"), NoLoop
}

func makePosIncr[T constraints.Integer](incr int64) T {
	if incr < 0 {
		return T(-incr)
	}
	return T(incr)
}

func advanceSeq[T constraints.Integer](lsn, minv, maxv, incrv T,
	cycle, minus, setEdge bool, db, tblname string, sg *SeqGenerator) (T, error) {
	// SetEdge means the incr value is so big
	// that every single update will just reset the lsn to the edge value.
	// But the start value may not be the edge value.
	if setEdge {
		// Set lastseqnum to maxv when this is a descending sequence.
		if minus {
			return setSeq(maxv, db, tblname, sg)
		}
		// Set lastseqnum to minv
		return setSeq(minv, db, tblname, sg)
	}

	cacheSize := T(sg.CacheSize)
	cachedNumRange := incrv * cacheSize
	// Now ignore the circumstance that cachedNumRange is bigger than one round of
	// [min, max]. If it is bigger, just make it smaller.
	if cachedNumRange > (maxv - minv) {
		cachedNumRange = maxv - minv
	}
	// There will be two side cases for cache.
	// case 1.
	// For cache that cycle is false, the final cache may not be big
	// enough to be cacheSize, we will just cache the remained values.
	// case 2.
	// For cache that cycle is true, after reaching the edge.The cached
	// max value may be smaller than the current value(for ascending sequence).
	var adseq T
	if minus {
		adseq = lsn - cachedNumRange
	} else {
		adseq = lsn + cachedNumRange
	}

	// check if descending sequence reaching edge
	if minus && (adseq < minv || adseq > lsn) {
		if !cycle {
			// case 1.
			return setSeq(minv, db, tblname, sg)
		}
		// case 2.
		// Just set to adseq.
	}
	// check if ascending sequence reaching edge
	if !minus && (adseq > maxv || adseq < lsn) {
		if !cycle {
			// case 1.
			return setSeq(maxv, db, tblname, sg)
		}
		// case 2.
	}
	return setSeq(adseq, db, tblname, sg)
}

func setSeq[T constraints.Integer](setv T, db, tbl string, sg *SeqGenerator) (T, error) {
	if err := sg.BgHandler.Exec(sg.Ctx, fmt.Sprintf(updateSeq, db, tbl, setv)); err != nil {
		return 0, err
	} else {
		return setv, nil
	}
}

func makeSeqCache[T constraints.Integer](v, lsn, minv, maxv, startv T, incrv int64, cycle, iscalled bool) *SeqCache {
	var sc SeqCache

	sc.cycle = cycle
	sc.iscalled = iscalled
	sc.incr = incrv

	sc.minv = int64(minv)
	sc.maxv = int64(maxv)
	sc.startv = int64(startv)

	sc.lsn = int64(lsn)
	sc.maxsn = int64(v)

	return &sc
}
