package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"
	"toy-raft/network"
	"toy-raft/state"
)

const (
	updateChannelBufferSz          int           = 10000
	heartbeatInterval              time.Duration = 1 * time.Second
	aeResponseTimeoutDuration      time.Duration = 200 * time.Millisecond
	voteResponseTimeoutDuration    time.Duration = 3 * time.Second
	maxElectionTimeout             time.Duration = 6 * time.Second
	minElectionTimeout             time.Duration = 5 * time.Second
	initialElectionTimeoutDuration time.Duration = 1 * time.Second
)

type RaftOperation interface {
	Bytes() []byte
	OpType() OperationType
}

type RaftNodeImpl struct {
	id           string
	stateMachine state.StateMachine

	quitCh chan bool

	inboundMessages chan []byte

	network network.Network

	// -- RAFT -- //
	state   RaftState
	storage Storage
	// set of peers including self
	peers map[string]bool

	// Follower
	electionTimeoutTimer *time.Timer

	// Candidate
	voteMap                  map[string]bool
	voteResponseTimeoutTimer *time.Timer

	// Leader only
	followersStateMap       map[string]*FollowerState
	sendAppendEntriesTicker *time.Ticker

	// All servers
	// index of highest log entry known to be committed
	commitIndex uint64
	// index of highest log entry applied to state machine
	lastApplied uint64
}

func NewRaftNodeImpl(id string, sm state.StateMachine, storage Storage, network network.Network, peers []string) *RaftNodeImpl {
	peersMap := make(map[string]bool, len(peers))
	for _, peer := range peers {
		peersMap[peer] = true
	}
	return &RaftNodeImpl{
		id:              id,
		storage:         storage,
		stateMachine:    sm,
		network:         network,
		quitCh:          make(chan bool),
		inboundMessages: make(chan []byte, updateChannelBufferSz),
		peers:           peersMap,
		state:           Follower,
		commitIndex:     0,
		lastApplied:     0,
	}
}

func parseMessage(messageBytes []byte) (OperationType, any, error) {
	var envelope Envelope
	if err := json.Unmarshal(messageBytes, &envelope); err != nil {
		return 0, nil, err
	}

	var message any
	switch envelope.OperationType {
	case VoteRequestOp:
		message = &VoteRequest{}
	case VoteResponseOp:
		message = &VoteResponse{}
	case AppendEntriesRequestOp:
		message = &AppendEntriesRequest{}
	case AppendEntriesResponseOp:
		message = &AppendEntriesResponse{}
	default:
		return 0, nil, fmt.Errorf("unknown operation type %d", envelope.OperationType)
	}
	if err := json.Unmarshal(envelope.Payload, message); err != nil {
		panic(err)
	}

	return envelope.OperationType, message, nil
}

func (rn *RaftNodeImpl) Start() {
	randomElectionTimeoutDuration := randomTimerDuration(minElectionTimeout, maxElectionTimeout)
	rn.electionTimeoutTimer = time.NewTimer(randomElectionTimeoutDuration)

	// init and stop
	rn.voteResponseTimeoutTimer = time.NewTimer(voteResponseTimeoutDuration)
	stopAndDrainTimer(rn.voteResponseTimeoutTimer)
	rn.sendAppendEntriesTicker = time.NewTicker(heartbeatInterval)
	stopAndDrainTicker(rn.sendAppendEntriesTicker)

	go func() {
		for {
			select {
			case <-rn.quitCh:
				return
			default:
				rn.processOneTransistion()
			}
		}
	}()
}

func (rn *RaftNodeImpl) processOneTransistion() {
	rn.processOneTransistionInternal(100 * time.Second)
}

func (rn *RaftNodeImpl) processOneTransistionInternal(inactivityTimeout time.Duration) {
	// get current term before we process a message
	currentTerm := rn.storage.GetCurrentTerm()

	select {
	case inboundMessage := <-rn.inboundMessages:
		// handle the new message from network
		opType, message, err := parseMessage(inboundMessage)
		if err != nil {
			rn.Log("bad message: %s", err)
			return
		}

		switch opType {
		case AppendEntriesRequestOp:
			appendEntriesRequest := message.(*AppendEntriesRequest)
			rn.Log("received AppendEntries request from %s, leader term: %d, lastLogIdx: %d, lastLogTerm: %d, leaderCommitIdx: %d", appendEntriesRequest.LeaderId, appendEntriesRequest.Term, appendEntriesRequest.PrevLogIdx, appendEntriesRequest.PrevLogTerm, appendEntriesRequest.LeaderCommitIdx)

			// peer is unknown, ignore request
			if !rn.isKnownPeer(appendEntriesRequest.LeaderId) {
				rn.Log("ignoring AppendEntries request from unknown peer: %s", appendEntriesRequest.LeaderId)
				return
			}

			// request has higher term, stepdown and update term
			if appendEntriesRequest.Term > currentTerm {
				rn.Log("stepping down and updating term (currentTerm: %d -> requestTerm: %d) due to AppendEntries request having a higher term", currentTerm, appendEntriesRequest.Term)
				// set new term to append entries request term
				rn.stepdownDueToHigherTerm(appendEntriesRequest.Term)
				// refresh value
				currentTerm = rn.storage.GetCurrentTerm()
			}

			resp := &AppendEntriesResponse{
				Term:        currentTerm,
				Success:     false,
				ResponderId: rn.id,
				MatchIndex:  0,
			}

			// request's term is lower than current term, deny request
			if appendEntriesRequest.Term < currentTerm {
				rn.SendMessage(appendEntriesRequest.LeaderId, resp)
				return
			} else if rn.state == Follower {
				// refresh election timer
				// NOTE: any state can receive an AE req, but the timer should only be reset for a follower
				resetAndDrainTimer(rn.electionTimeoutTimer, randomTimerDuration(minElectionTimeout, maxElectionTimeout))
			} else if rn.state == Candidate {
				// stepdown if a leader with the same term as us is found
				rn.stepdown()
			}

			// check if log state is consistent with leader
			if appendEntriesRequest.PrevLogIdx > 0 {
				// no entry exists
				entry, exists := rn.storage.GetLogEntry(appendEntriesRequest.PrevLogIdx)
				if !exists {
					rn.Log("found non-existent log entry at index %d when comparing with leader", appendEntriesRequest.PrevLogIdx)
					resp.Success = false
					rn.SendMessage(appendEntriesRequest.LeaderId, resp)
					return
				} else if entry.Term != appendEntriesRequest.PrevLogTerm {
					rn.Log("discovered log inconsistency with leader at index %d, expected term %d, got term %d", appendEntriesRequest.PrevLogIdx, appendEntriesRequest.PrevLogTerm, entry.Term)
					resp.Success = false
					rn.SendMessage(appendEntriesRequest.LeaderId, resp)
					return
				}
			}

			// append entries from request
			logEntryToBeAddedIdx := appendEntriesRequest.PrevLogIdx + 1
			rn.Log("attempting to add %d entries to log starting at index %d", len(appendEntriesRequest.Entries), logEntryToBeAddedIdx)
			for i, entry := range appendEntriesRequest.Entries {
				logEntry, exists := rn.storage.GetLogEntry(logEntryToBeAddedIdx)
				if !exists {
					rn.Log("appending entry %d/%d (%+v) at index %d", i+1, len(appendEntriesRequest.Entries), entry, logEntryToBeAddedIdx)
					rn.storage.AppendEntry(&entry)
				} else if entry.Term != logEntry.Term {
					rn.Log("deleting entries from index %d", logEntryToBeAddedIdx)
					rn.storage.DeleteEntriesFrom(logEntryToBeAddedIdx)
					rn.Log("appending entry %d/%d (%+v) at index %d", i+1, len(appendEntriesRequest.Entries), entry, logEntryToBeAddedIdx)
					rn.storage.AppendEntry(&entry)
				} else {
					rn.Log("entry %d/%d, already exists at index %d", i+1, len(appendEntriesRequest.Entries), logEntryToBeAddedIdx)
				}
				logEntryToBeAddedIdx++
			}
			if err := rn.storage.Commit(); err != nil {
				panic(err)
			}

			// update commit index
			indexOfLastNewEntry := appendEntriesRequest.PrevLogIdx + uint64(len(appendEntriesRequest.Entries))
			if appendEntriesRequest.LeaderCommitIdx > rn.commitIndex {
				prevCommitIndex := rn.commitIndex
				rn.commitIndex = min(appendEntriesRequest.LeaderCommitIdx, indexOfLastNewEntry)
				// commit index should only increase monotonically
				if rn.commitIndex < prevCommitIndex {
					panic(fmt.Sprintf("commit index %d is less than previous commit index %d", rn.commitIndex, prevCommitIndex))
				}
			}

			resp.Success = true
			resp.MatchIndex = appendEntriesRequest.PrevLogIdx + uint64(len(appendEntriesRequest.Entries))

			rn.SendMessage(appendEntriesRequest.LeaderId, resp)

			if rn.commitIndex > rn.storage.GetLastLogIndex() {
				panic(fmt.Sprintf("commit index %d is greater than last log index %d", rn.commitIndex, rn.storage.GetLastLogIndex()))
			}

			// apply newly committed entries
			for i := rn.lastApplied + 1; i <= rn.commitIndex; i++ {
				entry, exists := rn.storage.GetLogEntry(i)
				if !exists || entry == nil {
					panic(fmt.Sprintf("no log entry at index %d", i))
				}
				rn.Log("applying entry %d to state machine", i)
				rn.applyUpdate(entry)
			}
			rn.lastApplied = rn.commitIndex

		case AppendEntriesResponseOp:
			appendEntriesResponse := message.(*AppendEntriesResponse)
			rn.Log("received append entries response from %s", appendEntriesResponse.ResponderId)

			if !rn.isKnownPeer(appendEntriesResponse.ResponderId) {
				rn.Log("ignoring append entries response from unknown peer: %s", appendEntriesResponse.ResponderId)
				return
			}

			if appendEntriesResponse.Term > currentTerm {
				// set new term to vote request term
				rn.Log("append entries response with a higher term: %d", appendEntriesResponse.Term)
				rn.stepdownDueToHigherTerm(appendEntriesResponse.Term)
				return
			}

			if rn.state != Leader {
				rn.Log("ignoring append entries response as not leader")
				return
			}

			if appendEntriesResponse.Term < currentTerm {
				rn.Log("ignoring append entries response with a lower term: %d", appendEntriesResponse.Term)
				return
			}

			followerState, exists := rn.followersStateMap[appendEntriesResponse.ResponderId]
			if !exists {
				panic(fmt.Sprintf("responder %s is a valid peer but was not found in followers state map", appendEntriesResponse.ResponderId))
			}

			// successfully received a response from this follower
			followerState.waitingForAEResponse = false

			matchIndexUpdated := false
			if appendEntriesResponse.Success {
				if appendEntriesResponse.MatchIndex < followerState.matchIndex {
					panic(fmt.Sprintf("match index %d is less than follower match index %d", appendEntriesResponse.MatchIndex, followerState.matchIndex))
				} else if followerState.matchIndex == appendEntriesResponse.MatchIndex {
					// match index didn't change
				} else {
					matchIndexUpdated = true
					followerState.matchIndex = appendEntriesResponse.MatchIndex
					followerState.nextIndex = followerState.matchIndex + 1
				}
			} else {
				// NOTE: this only executes if log doesn't match
				if followerState.nextIndex == 1 {
					panic("cannot decrement nextIndex for a follower below 1")
				}
				followerState.nextIndex -= 1
				if followerState.nextIndex <= followerState.matchIndex {
					panic("nextIndex must be greater than matchIndex")
				}

				prevLogIndex := followerState.nextIndex - 1
				prevLogEntry, exists := rn.storage.GetLogEntry(prevLogIndex)
				if !exists {
					panic(fmt.Sprintf("no log entry at index %d", prevLogIndex))
				}

				entries := rn.entriesToSendToFollower(appendEntriesResponse.ResponderId)
				rn.sendNewAppendEntryRequest(&AppendEntriesRequest{
					Term:            currentTerm,
					LeaderId:        rn.id,
					Entries:         entries,
					PrevLogIdx:      prevLogIndex,
					PrevLogTerm:     prevLogEntry.Term,
					LeaderCommitIdx: rn.commitIndex,
				}, appendEntriesResponse.ResponderId, followerState)
			}

			// commit index only is incremented if matchIndex has been changed
			if matchIndexUpdated {
				if currentTerm != rn.storage.GetCurrentTerm() {
					panic(fmt.Sprintf("unexpected term change while handling AE response, expected: %d, actual: %d", currentTerm, rn.storage.GetCurrentTerm()))
				}

				quorum := len(rn.peers)/2 + 1
				lastLogIndex := rn.storage.GetLastLogIndex()

				upperBound := min(appendEntriesResponse.MatchIndex, lastLogIndex)
				lowerBound := rn.commitIndex + 1

				for n := upperBound; n >= lowerBound; n-- {

					logEntry, exists := rn.storage.GetLogEntry(n)
					if !exists {
						panic(fmt.Sprintf("log entry at %d, doesn't exist", n))
					}

					// NOTE: as an optimization we could just break here since it is guaranteed that all
					// entries previous to this will have lower terms than us
					if currentTerm != logEntry.Term {
						rn.Log("cannot set commitIndex to %d, term mismatch", n)
						continue
					}
					// count how many peer's log matches leader's upto N
					count := 0
					for _, followerState := range rn.followersStateMap {
						if followerState.matchIndex >= n {
							count++
						}
					}
					// majority of peers has entry[n], commit entries up to N
					if count >= quorum {
						rn.commitIndex = n
						rn.Log("commit index updated to %d", n)
						break
					}
				}
			}

		case VoteRequestOp:
			voteRequest := message.(*VoteRequest)
			rn.Log("received vote request from %s", voteRequest.CandidateId)

			lastLogIndex, lastLogEntryTerm := rn.storage.GetLastLogIndexAndTerm()

			if !rn.isKnownPeer(voteRequest.CandidateId) {
				rn.Log("ignoring vote request from unknown peer: %s", voteRequest.CandidateId)
				return
			}

			if voteRequest.Term > currentTerm {
				// set new term to vote request term
				rn.Log("vote request with a higher term, currentTerm: %d, voteRequestTerm: %d", currentTerm, voteRequest.Term)
				rn.stepdownDueToHigherTerm(voteRequest.Term)
				// refresh value
				currentTerm = rn.storage.GetCurrentTerm()
			}

			var voteGranted bool
			if voteRequest.Term < currentTerm {
				rn.Log("vote not granted to %s, voteRequestTerm %d < currentTerm %d", voteRequest.CandidateId, voteRequest.Term, currentTerm)
				voteGranted = false
			} else if rn.storage.Voted() && rn.storage.GetVotedFor() != voteRequest.CandidateId {
				rn.Log("vote not granted to %s, already voted for %s in term %d", voteRequest.CandidateId, rn.storage.GetVotedFor(), rn.storage.GetCurrentTerm())
				voteGranted = false
			} else if lastLogEntryTerm > voteRequest.LastLogTerm {
				rn.Log("vote not granted to %s, lastLogTerm %d > voteRequestLastLogTerm %d", voteRequest.CandidateId, lastLogEntryTerm, voteRequest.LastLogTerm)
				voteGranted = false
			} else if lastLogEntryTerm == voteRequest.LastLogTerm && lastLogIndex > voteRequest.LastLogIndex {
				rn.Log("vote not granted to %s, lastLogIndex %d > voteRequestLastLogIndex %d with same term %d", voteRequest.CandidateId, lastLogIndex, voteRequest.LastLogIndex, lastLogEntryTerm)
				voteGranted = false
			} else if rn.storage.Voted() && rn.storage.GetVotedFor() == voteRequest.CandidateId {
				rn.Log("already voted %s for them in term: %d, granted vote anyway", voteRequest.CandidateId, currentTerm)
				voteGranted = true
			} else {
				rn.Log("granted vote to %s with term %d", voteRequest.CandidateId, voteRequest.Term)
				voteGranted = true
				rn.storage.VoteFor(voteRequest.CandidateId, voteRequest.Term)
			}

			// do not reset timer for useless vote requests
			if voteGranted {
				resetAndDrainTimer(rn.electionTimeoutTimer, randomTimerDuration(minElectionTimeout, maxElectionTimeout))
			}

			// send vote response to candidate
			resp := &VoteResponse{
				Term:        currentTerm,
				VoteGranted: voteGranted,
				VoterId:     rn.id,
			}
			rn.SendMessage(voteRequest.CandidateId, resp)
		case VoteResponseOp:
			voteResponse := message.(*VoteResponse)

			if !rn.isKnownPeer(voteResponse.VoterId) {
				rn.Log("ignoring vote response from unknown peer: %s", voteResponse.VoterId)
				return
			}

			currentTerm = rn.storage.GetCurrentTerm()
			if voteResponse.Term > currentTerm {
				rn.Log("received vote response with a higher term, voteResponseTerm: %d", voteResponse.Term)
				rn.stepdownDueToHigherTerm(voteResponse.Term)
				return
			} else if voteResponse.Term < currentTerm {
				rn.Log("ignoring vote response from previous term %d", voteResponse.Term)
				return
			} else {
				rn.Log("received vote response from %s", voteResponse.VoterId)
			}

			// if we are not candidate, ignore
			if rn.state != Candidate {
				rn.Log("ignoring vote response, not a candidate")
				return
			}

			if !voteResponse.VoteGranted {
				rn.Log("voter %s voted no", voteResponse.VoterId)
				return
			}

			_, exists := rn.voteMap[voteResponse.VoterId]
			if exists {
				rn.Log("received duplicate vote from %s", voteResponse.VoterId)
				return
			}

			// add vote to map
			rn.Log("recording vote from %s", voteResponse.VoterId)
			rn.voteMap[voteResponse.VoterId] = true

			voteCount := len(rn.voteMap)
			numPeers := len(rn.peers)

			// majority
			if voteCount >= (numPeers/2)+1 {
				rn.ascendToLeader()
			}

		}

	case <-rn.electionTimeoutTimer.C:
		if rn.state != Follower {
			panic(fmt.Sprintf("election timeout while in state %s", rn.state))
		}
		rn.Log("election timeout, converting to candidate")
		rn.convertToCandidate()

	case <-rn.voteResponseTimeoutTimer.C:
		if rn.state != Candidate {
			panic(fmt.Sprintf("vote response timeout while in state %s", rn.state))
		}
		rn.convertToCandidate()

	case <-rn.sendAppendEntriesTicker.C:
		if rn.state != Leader {
			panic(fmt.Sprintf("send append entries ticker fired in state %s", rn.state))
		}
		for followerId, followerState := range rn.followersStateMap {
			var d time.Duration
			var aeReqType string
			if followerState.waitingForAEResponse {
				d = aeResponseTimeoutDuration
				aeReqType = "retry append entries request"
			} else {
				d = heartbeatInterval
				aeReqType = "heartbeat"
			}

			if time.Since(followerState.aeTimestamp) > d {
				var prevLogTerm uint64
				prevLogIndex := followerState.nextIndex - 1
				if prevLogIndex == 0 {
					prevLogTerm = 0
				} else {
					prevLogEntry, exists := rn.storage.GetLogEntry(prevLogIndex)
					if !exists {
						panic(fmt.Sprintf("no log entry at index %d", prevLogIndex))
					}
					prevLogTerm = prevLogEntry.Term
				}

				rn.sendNewAppendEntryRequest(&AppendEntriesRequest{
					Term:            currentTerm,
					LeaderId:        rn.id,
					Entries:         rn.entriesToSendToFollower(followerId),
					PrevLogIdx:      prevLogIndex,
					PrevLogTerm:     prevLogTerm,
					LeaderCommitIdx: rn.commitIndex,
				}, followerId, followerState)
				rn.Log("sent %s to %s", aeReqType, followerId)
			}
		}

	case <-time.After(inactivityTimeout):
		// inactivity
	}
}

func (rn *RaftNodeImpl) isKnownPeer(peerId string) bool {
	_, peerExists := rn.peers[peerId]
	return peerExists
}

func (rn *RaftNodeImpl) ascendToLeader() {
	if rn.state != Candidate {
		panic(fmt.Sprintf("%s attempted to transition to leader when not previously a candidate", rn.state))
	}
	if rn.followersStateMap != nil {
		panic("followersStateMap is not nil")
	}

	rn.Log("ascending to leader")

	// transistion to leader
	rn.state = Leader
	// clear vote map
	rn.voteMap = nil
	// stop voteResponseTimeoutTimer
	stopAndDrainTimer(rn.voteResponseTimeoutTimer)
	// stop electionTimeoutTimer
	stopAndDrainTimer(rn.electionTimeoutTimer)

	rn.followersStateMap = make(map[string]*FollowerState, len(rn.peers))
	for peerId := range rn.peers {
		rn.followersStateMap[peerId] = &FollowerState{
			nextIndex:  rn.storage.GetLastLogIndex() + 1,
			matchIndex: 0,
		}
	}

	// find term for last log entry, if no entries exist then 0
	var prevLogTerm uint64 = 0
	prevLogIdx := rn.storage.GetLastLogIndex()
	// log is not empty
	if prevLogIdx > 0 {
		lastLogEntry, exists := rn.storage.GetLogEntry(prevLogIdx)
		if !exists {
			panic("last log entry does not exist")
		}
		prevLogTerm = lastLogEntry.Term
	}

	// broadcast initial empty AppendEntriesRequest to peers
	newLeaderAEReq := &AppendEntriesRequest{
		Term:            rn.storage.GetCurrentTerm(),
		LeaderId:        rn.id,
		Entries:         []Entry{},
		PrevLogIdx:      prevLogIdx,
		PrevLogTerm:     prevLogTerm,
		LeaderCommitIdx: rn.commitIndex,
	}
	envelope := Envelope{
		OperationType: AppendEntriesRequestOp,
		Payload:       newLeaderAEReq.Bytes(),
	}
	rn.network.Broadcast(envelope.Bytes())
	rn.Log("broadcasted first AE requests, after becoming Leader, to followers")

	for _, followerState := range rn.followersStateMap {
		followerState.aeTimestamp = time.Now()
		followerState.waitingForAEResponse = true
	}

	resetAndDrainTicker(rn.sendAppendEntriesTicker, aeResponseTimeoutDuration)
}

// sends message, sets ae timestamp to now and sets waiting for response to true
func (rn *RaftNodeImpl) sendNewAppendEntryRequest(aeReq *AppendEntriesRequest, followerId string, followerState *FollowerState) {
	rn.SendMessage(followerId, aeReq)
	followerState.aeTimestamp = time.Now()
	followerState.waitingForAEResponse = true
}

func (rn *RaftNodeImpl) convertToCandidate() {
	rn.state = Candidate

	// candidate election timer
	resetAndDrainTimer(rn.voteResponseTimeoutTimer, voteResponseTimeoutDuration)
	// stop election timer
	stopAndDrainTimer(rn.electionTimeoutTimer)
	// stop followers append entries ticker
	stopAndDrainTicker(rn.sendAppendEntriesTicker)

	// reset vote map
	rn.voteMap = make(map[string]bool)
	// increment term
	currentTerm := rn.storage.IncrementTerm()
	// vote for self
	rn.storage.VoteFor(rn.id, currentTerm)
	rn.voteMap[rn.id] = true
	// request votes from other nodes
	rn.requestVotes(currentTerm, rn.id)
	rn.Log("converted to candidate, requested votes from other nodes")
}

// this method is triggered by receiving an RPC with a higher term, regardless of state
func (rn *RaftNodeImpl) stepdown() {

	var logMsg string
	previousState := rn.state

	if previousState == Leader {
		if rn.followersStateMap == nil {
			panic("a leader should have a followersStateMap")
		}
		rn.followersStateMap = nil
		stopAndDrainTicker(rn.sendAppendEntriesTicker)
		logMsg = "leader stepped down, cleared followersStateMap, and stopped sendAppendEntriesTicker"
	} else {
		if rn.followersStateMap != nil {
			panic(fmt.Sprintf("a %s should not have a followerStateMap", previousState))
		}
	}

	if previousState == Candidate {
		if rn.voteMap == nil {
			panic("a candidate should have a vote map")
		}
		// clear vote map
		rn.voteMap = nil
		// stop vote response timer
		stopAndDrainTimer(rn.voteResponseTimeoutTimer)
		logMsg = "candidate stepped down, cleared voteMap, and stopped voteResponseTimeoutTimer"
	} else {
		if rn.voteMap != nil {
			panic(fmt.Sprintf("a %s should not have a vote map", previousState))
		}
	}

	// convert to follower
	rn.state = Follower
	// reset election timer so we can give candidate time to resolve election
	resetAndDrainTimer(rn.electionTimeoutTimer, randomTimerDuration(minElectionTimeout, maxElectionTimeout))
	rn.Log(logMsg)
}

// this method is triggered by receiving an RPC with a higher term, regardless of state
func (rn *RaftNodeImpl) stepdownDueToHigherTerm(term uint64) {
	rn.stepdown()
	rn.storage.SetTerm(term)
}

func (rn *RaftNodeImpl) requestVotes(term uint64, candidateId string) {

	lastLogIndex, lastLogEntryTerm := rn.storage.GetLastLogIndexAndTerm()
	voteRequest := VoteRequest{
		Term:         term,
		CandidateId:  candidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogEntryTerm,
	}
	voteRequestBytes, err := json.Marshal(voteRequest)
	if err != nil {
		panic(err)
	}
	envelope := Envelope{
		OperationType: VoteRequestOp,
		Payload:       voteRequestBytes,
	}
	envelopeBytes, err := json.Marshal(envelope)
	if err != nil {
		panic(err)
	}
	rn.network.Broadcast(envelopeBytes)
}

func (rn *RaftNodeImpl) applyUpdate(update *Entry) {
	rn.stateMachine.Apply(update.Cmd)
}

func (rn *RaftNodeImpl) Stop() {
	rn.quitCh <- true
}

func (rn *RaftNodeImpl) Log(format string, args ...any) {

	var icon string
	switch rn.state {
	case Leader:
		icon = "👑"
	case Candidate:
		icon = "🗳️"
	default:
		icon = "🪵"
	}

	header := fmt.Sprintf("%s RAFT-%s (%s term:%d commit:%d applied:%d): ", icon, rn.id, rn.state, rn.storage.GetCurrentTerm(), rn.commitIndex, rn.lastApplied)
	log.Printf(header+format+"\n", args...)
}

var ErrNotLeader = fmt.Errorf("not leader")

// TODO: route to leader
func (rn *RaftNodeImpl) Propose(msg []byte) error {
	if rn.state == Leader {

		msgCopy := make([]byte, len(msg))
		if copy(msgCopy, msg) != len(msg) {
			panic("wtf")
		}

		// encode
		entry := &Entry{
			Term: rn.storage.GetCurrentTerm(),
			Cmd:  msgCopy,
		}
		rn.Log("proposing block of length %d", len(msg))
		rn.storage.AppendEntry(entry)
	} else {
		return ErrNotLeader
	}
	return nil
}

func (rn *RaftNodeImpl) Receive(msg []byte) {
	rn.inboundMessages <- msg
}

func (rn *RaftNodeImpl) SendMessage(peerId string, msg RaftOperation) {
	opType := msg.OpType()
	msgEnvelope := Envelope{
		OperationType: opType,
		Payload:       msg.Bytes(),
	}
	rn.Log("sending %s to %s: %+v", opType, peerId, msg)
	rn.network.Send(peerId, msgEnvelope.Bytes())
}

// If last log index ≥ nextIndex for a follower: send
// AppendEntries RPC with log entries starting at nextIndex
func (rn *RaftNodeImpl) entriesToSendToFollower(followerId string) []Entry {
	if rn.storage.GetLastLogIndex() >= rn.followersStateMap[followerId].nextIndex {
		return rn.storage.GetLogEntriesFrom(rn.followersStateMap[followerId].nextIndex)
	}
	return []Entry{}
}

func resetAndDrainTimer(t *time.Timer, resetDuration time.Duration) {
	stopAndDrainTimer(t)
	t.Reset(resetDuration)
}

func resetAndDrainTicker(t *time.Ticker, resetDuration time.Duration) {
	stopAndDrainTicker(t)
	t.Reset(resetDuration)
}

func stopAndDrainTimer(t *time.Timer) {
	t.Stop()
	for {
		select {
		case <-t.C:
		default:
			return
		}
	}
}

func stopAndDrainTicker(t *time.Ticker) {
	t.Stop()
	for {
		select {
		case <-t.C:
		default:
			return
		}
	}
}

func randomTimerDuration(min time.Duration, max time.Duration) time.Duration {
	return min + time.Duration(rand.Float64()*(float64(max-min)))
}
