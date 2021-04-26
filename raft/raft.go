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
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

func (pr *Progress) MaybeDecr(rejected uint64) bool {
	if pr.Next-1 != rejected {
		return false
	}

	pr.Next = max(rejected, 1)
	return true
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	state, confState, _ := c.Storage.InitialState()
	peers := c.peers
	if len(c.peers) <= 0 {
		peers = confState.Nodes
	}

	prs := make(map[uint64]*Progress, len(peers))
	for _, peer := range peers {
		prs[peer] = &Progress{}
	}

	raft := Raft{
		id:               c.ID,
		Term:             state.Term,
		Vote:             state.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		State:            StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	return &raft
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}

	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.votes = make(map[uint64]bool)
	for id, pr := range r.Prs {
		pr.Next = r.RaftLog.LastIndex() + 1
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
		} else {
			pr.Match = 0
		}
	}
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}

	if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}

		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}

	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	return r.maybeSendAppend(to, true)
}

func (r *Raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.Prs[to]
	term, err := r.RaftLog.Term(pr.Next - 1)
	if err != nil {
		return r.maybeSendSnapshot()
	}

	entries, err := r.RaftLog.Slice(pr.Next, r.RaftLog.LastIndex()+1)
	if err != nil {
		return r.maybeSendSnapshot()
	}

	if len(entries) == 0 && !sendIfEmpty {
		return false
	}

	msgEntries := make([]*pb.Entry, 0, len(entries))
	for _, entry := range entries {
		toAppend := entry
		msgEntries = append(msgEntries, &toAppend)
	}

	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgAppend,
		Index:   pr.Next - 1,
		LogTerm: term,
		Entries: msgEntries,
		Commit:  r.RaftLog.committed,
	}

	r.send(m)
	return true
}

func (r *Raft) maybeSendSnapshot() bool {
	panic("TODO")
}

func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	m := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	}

	r.send(m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.tickLeader()
	case StateCandidate:
		r.tickCandidate()
	case StateFollower:
		r.tickFollower()
	}
}

func (r *Raft) tickLeader() {
	if r.State != StateLeader {
		log.Panicf("Cannot tick leader for state: %d", r.State)
	}

	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) tickFollower() {
	if r.State != StateFollower {
		log.Panicf("Cannot tick follower for state: %d", r.State)
	}

	r.electionElapsed++
	if r.electionElapsed > r.randomizedElectionTimeout {
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickCandidate() {
	if r.State != StateCandidate {
		log.Panicf("Cannot tick candidate for state: %d", r.State)
	}

	r.electionElapsed++
	if r.electionElapsed > r.randomizedElectionTimeout {
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}

	r.reset(r.Term + 1)
	r.Vote = r.id
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower {
		log.Panic("invalid transition [follower -> leader]")
	}

	r.reset(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(emptyEnt) {
		log.Panicf("Error to append a empty entry")
	}

	log.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *Raft) appendEntry(es ...pb.Entry) (accepted bool) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}

	li = r.RaftLog.Append(es...)
	r.Prs[r.id].MaybeUpdate(li)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommit()
	return true
}

func (r *Raft) maybeCommit() bool {
	indexes := make([]uint64, 0, len(r.Prs))
	for _, prs := range r.Prs {
		indexes = append(indexes, prs.Match)
	}

	sort.SliceStable(indexes, func(i, j int) bool { return i > j })
	return r.RaftLog.MaybeCommit(indexes[len(r.Prs)/2], r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		// drop message
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgRequestVote:
		canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
		if canVote && r.RaftLog.IsUpToDate(m.Index, m.LogTerm) {
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	}
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}

	panic(fmt.Sprintf("Invalid state: %d", r.State))
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.hup()
	case pb.MessageType_MsgHeartbeat:
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.To, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVoteResponse:
		result := r.applyVote(m.From, !m.Reject)
		if result == VoteSuccess {
			r.becomeLeader()
			r.bcastAppend()
			return nil
		}

		if result == VoteFailure {
			r.becomeFollower(r.Term, None)
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	pr := r.Prs[m.From]
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			if pr.MaybeDecr(m.Index) {
				r.sendAppend(m.From)
			}
		} else {
			if pr.MaybeUpdate(m.Index) {
				if r.maybeCommit() {
					r.bcastAppend()
				}
			}
		}
	}
	return nil
}

func (r *Raft) hup() {
	if r.State == StateLeader {
		log.Warningf("%x ignoring MsgHup because already leader", r.id)
		return
	}

	entries := r.RaftLog.nextEnts()
	if n := numOfPendingConf(entries); n > 0 {
		log.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
		return
	}

	r.becomeCandidate()
	if result := r.applyVote(r.id, true); result == VoteSuccess {
		r.becomeLeader()
		return
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{To: id, MsgType: pb.MessageType_MsgRequestVote, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()})
	}
}

type VoteResult uint8

const (
	VoteSuccess VoteResult = iota
	VoteFailure
	VotePending
)

func (r *Raft) applyVote(id uint64, vote bool) VoteResult {
	r.votes[id] = vote
	accepted := 0
	denied := 0
	for _, v := range r.votes {
		if v {
			accepted += 1
		} else {
			denied += 1
		}
	}

	most := len(r.Prs) / 2

	if accepted > most {
		return VoteSuccess
	}

	if denied > most {
		return VoteFailure
	}

	return VotePending
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	entries := make([]pb.Entry, 0, len(m.Entries))
	for _, entry := range m.Entries {
		entries = append(entries, *entry)
	}

	if mlastIndex, ok := r.RaftLog.MaybeAppend(m.Index, m.LogTerm, m.Commit, entries...); ok {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: mlastIndex})
	} else {
		// Return a hint to the leader about the maximum index and term that the
		// two logs could be divergent at. Do this by searching through the
		// follower's log for the maximum (index, term) pair with a term <= the
		// MsgApp's LogTerm and an index <= the MsgApp's Index. This can help
		// skip all indexes in the follower's uncommitted tail with terms
		// greater than the MsgApp's LogTerm.
		//
		// See the other caller for findConflictByTerm (in stepLeader) for a much
		// more detailed explanation of this mechanism.
		hintIndex := min(m.Index, r.RaftLog.LastIndex())
		hintIndex = r.RaftLog.findConflictByTerm(hintIndex, m.LogTerm)
		hintTerm, err := r.RaftLog.Term(hintIndex)
		if err != nil {
			panic(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
		}
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   m.Index,
			Reject:  true,
			LogTerm: hintTerm,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].EntryType == pb.EntryType_EntryConfChange {
			n++
		}
	}
	return n
}
