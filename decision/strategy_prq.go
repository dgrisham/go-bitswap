// Strategy PRQ
// ============

package decision

import (
	"sync"
	"time"

	wantlist "github.com/ipfs/go-bitswap/wantlist"
	pq "github.com/ipfs/go-ipfs-pq"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-peer"
)

// Type and Constructor
// --------------------

func newSPRQ(rrqCfg *RRQConfig) *sprq {
	return &sprq{
		taskMap:  make(map[taskEntryKey]*peerRequestTask),
		partners: make(map[peer.ID]*activePartner),
		pQueue:   pq.New(partnerCompare),
		rrq:      newRRQueue(rrqCfg),
	}
}

// verify interface implementation
var _ peerRequestQueue = &sprq{}

type sprq struct {
	lock     sync.Mutex
	pQueue   pq.PQ
	taskMap  map[taskEntryKey]*peerRequestTask
	partners map[peer.ID]*activePartner
	rrq      *RRQueue
}

func (tl *sprq) TaskMap() map[taskEntryKey]*peerRequestTask {
	return tl.taskMap
}

func (tl *sprq) GetActivePartner(id peer.ID) *activePartner {
	return tl.partners[id]
}

// Push
// ----

// Push adds a new peerRequestTask to the end of the list
func (tl *sprq) Push(receipt *Receipt, entries ...*wantlist.Entry) {
	to := peer.ID(receipt.Peer)
	tl.lock.Lock()
	defer tl.lock.Unlock()

	partner, ok := tl.partners[to]

	if !ok {
		partner = newActivePartner()
		tl.pQueue.Push(partner)
		tl.partners[to] = partner
	}

	partner.activelk.Lock()
	defer partner.activelk.Unlock()

	var priority int
	newEntries := make([]*wantlist.Entry, 0, len(entries))
	for _, entry := range entries {
		if partner.activeBlocks.Has(entry.Cid) {
			return
		}

		if task, ok := tl.taskMap[taskEntryKey{to, entry.Cid}]; ok {
			if entry.Priority > task.Priority {
				task.Priority = entry.Priority
				partner.taskQueue.Update(task.index)
			}
			continue
		}
		if entry.Priority > priority {
			priority = entry.Priority
		}
		newEntries = append(newEntries, entry)
	}

	if len(newEntries) == 0 {
		return
	}

	task := &peerRequestTask{
		Entries: newEntries,
		Target:  to,
		created: time.Now(),
		Done: func(e []*wantlist.Entry) {
			tl.lock.Lock()
			for _, entry := range e {
				partner.TaskDone(entry.Cid)
			}
			tl.pQueue.Update(partner.Index())
			tl.lock.Unlock()
		},
	}
	task.Priority = priority
	partner.taskQueue.Push(task)
	for _, entry := range newEntries {
		tl.taskMap[taskEntryKey{to, entry.Cid}] = task
	}
	partner.requests += len(newEntries)
	tl.pQueue.Update(partner.Index())

	// TODO: figure out if this needs to change for the 'entry -> entries' update
	tl.rrq.UpdateWeight(to, receipt)
}

// Pop
// ---

// Pop uses the `RRQueue` and peer `taskQueue`s to determine the next request to serve
// TODO: confirm that this is all right (try to compile, check logic (run tests), etc.)
func (tl *sprq) Pop() *peerRequestTask {
	tl.lock.Lock()
	defer tl.lock.Unlock()

	if tl.pQueue.Len() == 0 {
		return nil
	}
	if tl.rrq.NumPeers() == 0 {
		// may have finished last RR round, reallocate requests to peers
		tl.rrq.InitRound()
		if tl.rrq.NumPeers() == 0 {
			// if allocations still empty, there are no requests to serve
			return nil
		}
	}

	// figure out which peer should be served next
	for tl.rrq.NumPeers() > 0 {
		rrp := tl.rrq.Head()
		partner := tl.partners[rrp.id]
		for partner.taskQueue.Len() > 0 {
			out := partner.taskQueue.Pop().(*peerRequestTask)
			var newEntries []*wantlist.Entry
			// rem holds the remaining entries that we don't end up sending this round
			rem := new(peerRequestTask)
			*rem = *out
			rem.Entries = make([]*wantlist.Entry, len(out.Entries))
			copy(rem.Entries, out.Entries)

			remove := 0
			for _, entry := range out.Entries {
				if entry.Trash {
					delete(tl.taskMap, taskEntryKey{out.Target, entry.Cid})
					remove++
					continue
				}
				// check whether serving this entry will exceed the RR allocation
				if entry.Size <= rrp.allocation {
					delete(tl.taskMap, taskEntryKey{out.Target, entry.Cid})
					partner.requests--
					partner.StartTask(entry.Cid)
					newEntries = append(newEntries, entry)
					rrp.allocation -= entry.Size
					if rrp.allocation == 0 {
						// peer has reached allocation limit for this round, remove peer from queue
						tl.rrq.Pop()
						break
					}
					remove++
				} else {
					break
				}
			}
			// cut off all of the entries that are being sent from rem
			rem.Entries = rem.Entries[remove:]
			if len(rem.Entries) > 0 {
				// push the task with the remaining entries onto the peer's taskQueue
				partner.taskQueue.Push(rem)
			}
			if len(newEntries) > 0 {
				out.Entries = newEntries
			} else {
				out = nil
				tl.rrq.Pop()
				continue
			}
			return out
		}
	}
	return nil
}

// Remove
// ------

// Remove removes a task from the queue
func (tl *sprq) Remove(k cid.Cid, p peer.ID) {
	tl.lock.Lock()
	t, ok := tl.taskMap[taskEntryKey{p, k}]
	if ok {
		for _, entry := range t.Entries {
			if entry.Cid.Equals(k) {
				// remove the task "lazily"
				// simply mark it as trash, so it'll be dropped when popped off the
				// queue.
				entry.Trash = true
				break
			}
		}

		// having canceled a block, we now account for that in the given partner
		partner := tl.partners[p]
		partner.requests--

		// we now also 'freeze' that partner. If they sent us a cancel for a
		// block we were about to send them, we should wait a short period of time
		// to make sure we receive any other in-flight cancels before sending
		// them a block they already potentially have
		//
		// TODO: figure out how to implement this for RRQ, e.g. move partner to end of
		// RRQ so that they still get their allocations but go at the end of the round
		// (but then also have to decide what to do if partner only peer in queue
		/*if partner.freezeVal == 0 {
			tl.frozen[p] = partner
		}*/

		//partner.freezeVal++
		tl.pQueue.Update(partner.index)
	}
	tl.lock.Unlock()
}

// Unimplemented
// -------------

func (tl *sprq) thawRound() {
}

// Helpers
// -------

func (tl *sprq) allocationForPeer(id peer.ID) int {
	for _, rrp := range tl.rrq.allocations {
		if rrp.id == id {
			return rrp.allocation
		}
	}
	return 0
}
