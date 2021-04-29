// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	offsetEntry pb.Entry

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Panicf("error to get storage first index, %s", err.Error())
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Panicf("error to get storage last index, %s", err.Error())
	}

	committed := firstIndex - 1
	applied := firstIndex - 1

	hardState, _, err := storage.InitialState()
	if err != nil {
		log.Panicf("error to get storage initial state, %s", err.Error())
	}

	if !IsEmptyHardState(hardState) {
		committed = hardState.Commit
	}

	offsetEntry := pb.Entry{}
	offsetEntry.Index = firstIndex - 1
	offsetEntry.Term, _ = storage.Term(firstIndex - 1)

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		log.Panicf("error to get storage entries, %s", err.Error())
	}

	return &RaftLog{
		storage:     storage,
		committed:   committed,
		applied:     applied,
		stabled:     lastIndex,
		offsetEntry: offsetEntry,
		entries:     entries[:],
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	offset := l.offsetEntry.Index
	if l.applied > offset {
		l.entries = l.entries[l.applied-offset:]
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	offset := l.offsetEntry.Index
	return l.entries[l.stabled-offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	offset := l.offsetEntry.Index
	return l.entries[l.applied-offset : l.committed-offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return l.offsetEntry.Index + uint64(len(l.entries))
}

func (l *RaftLog) LastTerm() uint64 {
	t, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	offset := l.offsetEntry.Index
	if i < offset {
		return 0, ErrCompacted
	}

	if i == offset {
		return l.offsetEntry.Term, nil
	}

	if int(i-offset) > len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[i-offset-1].Term, nil
}

// Slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *RaftLog) Slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo > hi {
		return nil, errors.Errorf("invalid slice %d > %d", lo, hi)
	}

	offset := l.offsetEntry.Index
	if lo <= offset {
		return nil, ErrCompacted
	}

	if hi > l.LastIndex()+1 {
		return nil, errors.Errorf("slice[%d,%d) out of bound [%d,%d]", lo, hi, offset, l.LastIndex())
	}

	if lo == hi {
		return nil, nil
	}

	return l.entries[lo-offset-1 : hi-offset-1], nil
}

func (l *RaftLog) IsUpToDate(lasti, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && lasti >= l.LastIndex())
}

func (l *RaftLog) Append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	offset := l.offsetEntry.Index
	after := ents[0].Index - 1

	if after < l.committed || after > l.LastIndex() {
		log.Panicf("after(%d) is out of range [committed(%d), lastIndex(%d)]", after, l.committed, l.LastIndex())
	}

	if after == l.LastIndex() {
		l.entries = append(l.entries, ents...)
	} else if after >= l.offsetEntry.Index {
		l.entries = append([]pb.Entry{}, l.entries[:after-offset]...)
		l.entries = append(l.entries, ents...)
		if l.stabled > after {
			l.stabled = after
		}
	} else {
		l.entries = ents[l.offsetEntry.Index-after:]
		l.stabled = l.offsetEntry.Index
	}

	return l.LastIndex()
}

func (l *RaftLog) MaybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			l.Append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.Term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.LastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		log.Warningf("index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm",
			index, li)
		return index
	}
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) Snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}

	return l.storage.Snapshot()
}

func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
	}
}

func (l *RaftLog) MaybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	log.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *RaftLog) AppliedTo(i uint64) {
	if i == 0 {
		return
	}

	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *RaftLog) StableTo(i, t uint64) {
	if l.matchTerm(i, t) && i > l.stabled {
		l.stabled = i
	}
}

func (l *RaftLog) StableSnapTo(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}
