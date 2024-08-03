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
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"math/rand"
	"sort"
	"time"

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

	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic("newRaft failed: hardState error")
	}

	raftLog := newLog(c.Storage)

	// map[uint64]*Progress
	prs := make(map[uint64]*Progress)

	if len(c.peers) == 0 {
		c.peers = confState.Nodes
	}

	// 从 peers []uint64 构造这个 prs
	for _, peer := range c.peers {
		// 这里一开始先为 0, 到后面变成Leader后， Match = 0, Next = lastIndex + 1
		p := &Progress{
			Match: 0,
			Next:  0,
		}
		prs[peer] = p
	}

	raft := &Raft{
		id:      c.ID,
		Term:    hardState.Term,
		Vote:    hardState.Vote,
		RaftLog: raftLog,

		Prs:   prs,
		State: StateFollower,
		votes: make(map[uint64]bool),
		msgs:  nil,
		Lead:  None,

		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,

		// leadTransferee:
		// PendingConfIndex:
	}

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) {
	// Your Code Here (2A).
	nextIndex := r.Prs[to].Next
	// fmt.Printf("raft %v 的nextIndex是%v\n", to, nextIndex)
	prevLogIndex := nextIndex - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	firstIndex, _ := r.RaftLog.storage.FirstIndex()

	// 如果新加入节点,或者 follower大幅度落后leader,会出现日志找不到的情况（已经被截断)
	if nextIndex < firstIndex {
		// 发送快照
		// TODO()
		return
	}

	// 那么就可以从 RaftLog.Entries中发送
	var entries []*pb.Entry
	lastIndex := r.RaftLog.LastIndex()

	if nextIndex > lastIndex {
		// 不需要发送，已经是最新的了,
		// 真的吗，这个可能是用来更新 commit Index 的

	}
	// [nextIndex, lastIndex]都要发送
	// 0   1   2   3   4   5   6   7   8
	// f       n                       l

	for i := nextIndex; i <= lastIndex; i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}

	message := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,

		LogTerm: prevLogTerm,   // 对应论文的 prevLogTerm
		Index:   nextIndex - 1, // 对应论文的 prevLogIndex
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	// fmt.Printf("leader 发送一个appendEntries, commitIndex is %d\n", r.RaftLog.committed)
	r.msgs = append(r.msgs, message)

	return
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 这个函数只需要把心跳信心放入 raft.msg 中,再由上层调用HandleRaftReady函数发送就行
	message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  util.RaftInvalidIndex,
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) sendHeartbeatResponse(to uint64) {
	message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, message)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}

	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}

	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// 广播心跳   pb.Message
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				return
			}
		}
	}

}

func (r *Raft) resetRandomizedElectionTimeout() {
	rand.Seed(time.Now().UnixNano()) // 使用当前时间的纳秒数作为随机数种子
	randomInt := rand.Intn(10)
	// [10,20) 之间的数
	r.electionTimeout += randomInt
	for r.electionTimeout >= 20 {
		r.electionTimeout -= 10
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// 如果他给别的投了一票，然后又变成follower,但它的term没有变化，这里的vote该如何改变
	if r.Term != term {
		r.Vote = None
		r.votes = make(map[uint64]bool)
	}

	r.Term = term
	r.Lead = lead
	r.State = StateFollower

	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	// 随机化选举超时时间,避免选举时票数均匀导致多次选举
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 首先要把自己的 term + 1
	r.Term = r.Term + 1
	r.State = StateCandidate
	r.Vote = None

	// 根据这个判断有没有超过半数给自己投票,变成candidate的时候重置votes
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	r.Vote = r.id
	r.votes[r.id] = true

	r.resetRandomizedElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// fmt.Printf("[%d] 成为了 Leader, term %d\n", r.id, r.Term)
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.Lead = r.id
	r.heartbeatElapsed = 0
	// 需要更改 r.Prs
	lastIndex := r.RaftLog.LastIndex()
	for _, p := range r.Prs {
		p.Match = 0
		p.Next = lastIndex + 1
	}

	// 追加一条空日志
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: lastIndex + 1})
	// 更新自己的 match 和 next
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.Prs[r.id].Match = r.Prs[r.id].Next - 1

	// 向所有的节点发送
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}

	// 小 bug：这里需要判断一下是否需要推进 commitIndex,(有可能只有一个Leader节点)
	r.maybeUpdateCommitIndex()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	var err error = nil
	switch r.State {
	case StateFollower:
		err = r.stepFollower(m)
	case StateCandidate:
		err = r.stepCandidate(m)
	case StateLeader:
		err = r.stepLeader(m)
	}
	return err
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgAppend:
		// 收到一个AppendEntriesRPC
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHup:
		// 又一次选举超时....
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
		// 需要进行广播心跳
		for pr := range r.Prs {
			if r.id != pr {
				r.sendHeartbeat(pr)
			}
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgHup:
		// 什么都不要干
	}
	return nil
}

func (r *Raft) appendEntry(entries []*pb.Entry) {
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = r.RaftLog.LastIndex() + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entries[i])
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
}

func (r *Raft) handlePropose(m pb.Message) {
	// 在raftLog中添加日志
	r.appendEntry(m.Entries)

	// 这里不需要更新所有的其他节点的 matchIndex 和 nextIndex
	// 向所有的节点发送 AppendEntryRPC
	for i := range r.Prs {
		if i != r.id {
			r.sendAppend(i)
		}
	}

	// 如果这个 raft 集群只有 1 台机器，那么直接要进行提交
	if len(r.Prs) == 1 {
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) maybeUpdateCommitIndex() {
	// 根据 r.Prs中的 matchIndex 得出
	// 按照论文的思路更新 commit。
	// 即：假设存在 N 满足N > commitIndex，使得大多数的 matchIndex[i] ≥ N以及 log[N].term == currentTerm 成立，
	// 则令 commitIndex = N。为了快速更新，这里先将节点按照 match 进行了递增排序，这样就可以快速确定 N 的位置。
	oldCommit := r.RaftLog.committed
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, p := range r.Prs {
		match[i] = p.Match
		i++
	}
	sort.Sort(match)

	// 所以 leader 一定要记得更新自己的 matchIndex
	// number = 17   超过半数是 9
	// 1  2  3  4  4  4  5  6  7  7  8  9  10  10  10  10  20
	// 0  1  2  3  4  5  6  7  8  9  10 11 12  13  14  15  16
	// 要超过半数才能算 commit
	commitIndex := match[(len(r.Prs)-1)/2]
	// 从这条 commitIndex 开始往前推，找到第一条满足条件的就行 [r.Commit, commitIndex]
	for ; commitIndex > r.RaftLog.committed; commitIndex-- {
		term, _ := r.RaftLog.Term(commitIndex)
		if term == r.Term {
			break
		}
	}

	r.RaftLog.committed = commitIndex
	// fmt.Printf("leader commit index is %d\n", commitIndex)

	if r.RaftLog.committed != oldCommit {
		for peer := range r.Prs {
			if r.id != peer {
				r.sendAppend(peer)
			}
		}
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if !m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
		// 可能还需要更新 commitIndex
		r.maybeUpdateCommitIndex()
		return
	}

	// 否则需要重新发送
	// 重新发送如何更新 NextIndex
	r.Prs[m.From].Next -= 1
	r.sendAppend(m.From)
}

func (r *Raft) sendRequestVote(to uint64) {
	lastLogIndex := r.RaftLog.LastIndex()      // prevLogIndex
	logTerm, _ := r.RaftLog.Term(lastLogIndex) // prevLogTerm

	message := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   lastLogIndex, // prevLogIndex

		LogTerm: logTerm, // prevLogTerm
	}
	r.msgs = append(r.msgs, message)

	// fmt.Printf("sendRequestVote: msg的长度%d\n", len(r.msgs))
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// fmt.Printf("收到 RequestVote, %d -> %d \n", m.From, m.To)

	// 自己的 term 大于选举者的 term
	if r.Term > m.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}

	// 自己的 term 要小于等于选举者的 term
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None) // becomeFollower里面有Vote置为None的逻辑
	}

	// 其实还要比较日志进行一致性检查 TODO()
	if r.Vote == None || r.Vote == m.From {
		lastLogIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)

		if lastLogTerm > m.LogTerm {
			// 该 raft 的日志更新，拒绝投票
			r.sendRequestVoteResponse(m.From, true)
			return
		}
		if lastLogTerm == m.LogTerm {
			if lastLogIndex > m.Index {
				r.sendRequestVoteResponse(m.From, true)
				return
			}
		}

		r.Vote = m.From
		r.sendRequestVoteResponse(m.From, false)
		return
	}

	r.sendRequestVoteResponse(m.From, true)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	message := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		To:      to,
		From:    r.id,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, message)
}

func (r *Raft) startElection() {
	// 如果集群中只有一个节点，那么需要特殊处理，否则永远不会执行handleRequestVote函数，也就不会成为leader

	// fmt.Printf("[%d], term %d, 发动了一次选举\n", r.id, r.Term)

	if len(r.Prs) == 1 {
		r.becomeCandidate()
		r.becomeLeader()
		return
	}
	r.becomeCandidate()

	// 向每一个 raft 发送
	// fmt.Printf("raft[%d]'s Prs is %v\n", r.id, r.Prs)
	for p := range r.Prs {
		if p != r.id {
			// fmt.Printf("[%d]向[%d], term %d, 索要一次投票\n", r.id, p, r.Term)
			r.sendRequestVote(p)
		}
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// fmt.Printf("收到 RequestVoteResponse, %d -> %d  reject=%v\n", m.From, m.To, m.Reject)
	if m.Term < r.Term {
		return
	}

	if !m.Reject {
		r.votes[m.From] = true
		agreeNum := 0
		for _, granted := range r.votes {
			if granted == true {
				agreeNum++
			}
		}
		// 已经当选leader又一次获得选票时，由于状态的变更所以不会执行stepCandidate，所以也不会再次执行到这里
		if agreeNum >= len(r.Prs)/2+1 {
			r.becomeLeader()
		}
		return
	}

	r.votes[m.From] = false
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	// 如果 false 太多了是不是要切换为 follower
	disagreeNum := 0
	for _, granted := range r.votes {
		if granted == false {
			disagreeNum++
		}
	}
	// 7: dis=4   8:dis=4     9:dis=5
	if 2*disagreeNum >= len(r.Prs) {
		r.becomeFollower(r.Term, None)
	}

}

func (r *Raft) sendAppendEntriesResponse(to uint64, reject bool) {
	// fmt.Printf("发送 AppendEntriesResponse\n")
	message := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  reject,
	}

	r.msgs = append(r.msgs, message)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// fmt.Printf("来了一次appendEntries m.commit is %v, entries len is %d\n", m.Commit, len(m.Entries))

	// Your Code Here (2A).
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	if r.Term > m.Term {
		r.sendAppendEntriesResponse(m.From, true)
		return
	}

	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm
	if prevLogIndex > r.RaftLog.LastIndex() {
		// 说明该 raft 的日志不完整
		// 为了快速定位，该raft需要向leader告诉它自己的
		// Index	r.RaftLog.LastIndex()；该字段用于 Leader 更快地去更新 next
		r.sendAppendEntriesResponse(m.From, true)
		return
	}

	logTerm, _ := r.RaftLog.Term(prevLogIndex)
	if logTerm != prevLogTerm {
		// Index	r.RaftLog.LastIndex()；该字段用于 Leader 更快地去更新 next
		r.sendAppendEntriesResponse(m.From, true)
		return
	}

	// 现在可以为 raft 添加日志, 从 index=prevLogIndex + 1开始添加
	entryLen := len(m.Entries)
	lastAppend := m.Index

	for i := 0; i < entryLen; i++ {
		index := m.Entries[i].Index
		term := m.Entries[i].Term

		oldTerm, err1 := r.RaftLog.Term(index)
		firstIndex, _ := r.RaftLog.storage.FirstIndex()
		// 不存在这条日志,那么往后的日志直接append到r.RaftLog中
		if err1 != nil || index-firstIndex > uint64(len(r.RaftLog.entries)) {
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])

			// 记录一下新添加日志的 index
			lastAppend = m.Entries[i].Index
		} else if oldTerm != term {
			// 后面的日志需要重写
			// index 如果要小于 firstIndex 那该怎么办
			if index >= firstIndex {
				r.RaftLog.entries = r.RaftLog.entries[0 : index-firstIndex]
			} else {
				r.RaftLog.entries = make([]pb.Entry, 0)
			}

			// 添加日志
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])

			// stabled >= committed
			// leader 新加过来的日志还没有持久化, stabled 由 Ready 进行处理
			r.RaftLog.stabled = min(r.RaftLog.stabled, prevLogIndex)

			lastAppend = m.Entries[i].Index
		}

		lastAppend = m.Entries[i].Index
	}

	// 如何更新 commit index ?
	// 记录下当前追加的最后一个条目的 Index。
	// 比较 Leader 已知已经提交的最高的日志条目的索引 m.Commit 或者是上一个新条目的索引，然后取两者的最小值
	// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
	// 这里的 index of the last new entry !!!!! 并不等于 r.RaftLog.LastIndex
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = max(min(m.Commit, lastAppend), r.RaftLog.committed)
	}
	r.sendAppendEntriesResponse(m.From, false)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// 要发送自己的 term, commitIndex,
	if r.Term <= m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	// 转换 leader
	if m.From != r.Lead {
		r.Lead = m.From
	}
	r.electionElapsed = 0

	// fmt.Printf("%v 收到心跳\n", r.id)
	r.sendHeartbeatResponse(m.From)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// fmt.Printf("处理%v心跳回复\n", m.From)
	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(r.Term, None)
		}
	}
	// fmt.Printf("心跳回复: raft id is %v, commit is %v\n", m.From, m.Commit)
	if m.Commit < r.RaftLog.committed {
		// fmt.Printf("%v的 commit 落后了，需要 sendAppend\n", m.From)
		r.sendAppend(m.From)
	}
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
