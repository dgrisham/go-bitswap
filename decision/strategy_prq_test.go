// Strategy PRQ Tests
// ==================

package decision

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/ipfs/go-bitswap/wantlist"
	cid "github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	peer "github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-testutil"
)

// Single-Peer Tests
// -----------------

func TestSPRQPushPopLegacy(t *testing.T) {
	prq := newSPRQ(&RRQConfig{RoundBurst: 5000, Strategy: Identity})
	partner := testutil.RandPeerIDFatal(t)
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")
	vowels := strings.Split("aeiou", "")
	consonants := func() []string {
		var out []string
		for _, letter := range alphabet {
			skip := false
			for _, vowel := range vowels {
				if letter == vowel {
					skip = true
				}
			}
			if !skip {
				out = append(out, letter)
			}
		}
		return out
	}()
	sort.Strings(alphabet)
	sort.Strings(vowels)
	sort.Strings(consonants)

	// add a bunch of blocks. cancel some. drain the queue. the queue should only have the kept entries
	l := newLedger(partner).Receipt()
	l.Value = 1
	for _, index := range rand.Perm(len(alphabet)) { // add blocks for all letters
		letter := alphabet[index]
		c := cid.NewCidV0(u.Hash([]byte(letter)))
		prq.Push(l, &wantlist.Entry{Cid: c, Priority: math.MaxInt32 - index})
	}
	for _, consonant := range consonants {
		c := cid.NewCidV0(u.Hash([]byte(consonant)))
		prq.Remove(c, partner)
	}

	var out []string
	for {
		received := prq.Pop()
		if received == nil {
			break
		}
		for _, entry := range received.Entries {
			out = append(out, entry.Cid.String())
		}
	}

	// Entries popped should already be in correct order
	for i, expected := range vowels {
		exp := cid.NewCidV0(u.Hash([]byte(expected))).String()
		if out[i] != exp {
			t.Fatal("received", out[i], "expected", exp)
		}
	}
}

func TestSPRQPushPopServeAllMultipleTasks(t *testing.T) {
	roundBurst := 100
	prq := newSPRQ(&RRQConfig{RoundBurst: roundBurst, Strategy: Identity})
	partner := testutil.RandPeerIDFatal(t)
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")

	l := newLedger(partner).Receipt()
	l.Value = 1
	blockSize := 5
	for index, letter := range alphabet { // add blocks for all letters
		c := cid.NewCidV0(u.Hash([]byte(letter)))
		prq.Push(l, &wantlist.Entry{Cid: c, Priority: math.MaxInt32 - index, Size: blockSize})
	}

	expectedAllocation := roundBurst
	var out []string
	for {
		received := prq.Pop()
		if received == nil {
			break
		}
		if expectedAllocation == 0 {
			expectedAllocation = roundBurst
		}
		expectedAllocation -= blockSize
		if prq.allocationForPeer(partner) != expectedAllocation {
			t.Fatalf("Expected allocation of %d, got %d", expectedAllocation, prq.allocationForPeer(partner))
		}
		out = append(out, received.Entries[0].Cid.String())
	}

	if expectedAllocation != 70 {
		t.Fatalf("Peer should have ended with 70 allocation, but had %d", expectedAllocation)
	}
	if len(out) != len(alphabet) {
		t.Fatalf("Expected %d blocks popped, got %d", len(alphabet), len(out))
	}
	for i, expected := range alphabet {
		exp := cid.NewCidV0(u.Hash([]byte(expected))).String()
		if out[i] != exp {
			t.Fatalf("Expected %s, received %s", exp, out[i])
		}
	}
}

func TestSPRQPushPopServeAllOneTask(t *testing.T) {
	roundBurst := 100
	prq := newSPRQ(&RRQConfig{RoundBurst: roundBurst, Strategy: Identity})
	partner := testutil.RandPeerIDFatal(t)
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")

	l := newLedger(partner).Receipt()
	l.Value = 1
	blockSize := 5
	entries := make([]*wantlist.Entry, 0, len(alphabet))
	for index, letter := range alphabet { // add blocks for all letters
		c := cid.NewCidV0(u.Hash([]byte(letter)))
		entries = append(entries, &wantlist.Entry{Cid: c, Priority: math.MaxInt32 - index, Size: blockSize})
	}
	prq.Push(l, entries...)

	received := prq.Pop()
	var out []string
	for _, entry := range received.Entries {
		out = append(out, entry.Cid.String())
	}

	if prq.allocationForPeer(partner) != 0 {
		t.Fatalf("Peer should have ended with 0 allocation, but had %d", prq.allocationForPeer(partner))
	}
	if len(out) != roundBurst/blockSize {
		t.Fatalf("Expected %d blocks popped, got %d", len(alphabet), len(out))
	}
	for i := 0; i < roundBurst/blockSize; i++ {
		if i >= roundBurst/blockSize {
			// TODO
			break
		}
		expected := cid.NewCidV0(u.Hash([]byte(alphabet[i]))).String()
		if out[i] != expected {
			t.Fatalf("Expected %s, received %s", expected, out[i])
		}
	}
}

func TestSPRQPushPop1Round(t *testing.T) {
	prq := newSPRQ(&RRQConfig{RoundBurst: 100, Strategy: Identity})
	partner := testutil.RandPeerIDFatal(t)
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")
	// the first 20 letters should be served by the end
	expectedOut := strings.Split("abcdefghijklmnopqrst", "")
	expectedRemaining := strings.Split("uvwxyz", "")

	l := newLedger(partner).Receipt()
	l.Value = 1
	blockSize := 5
	for index, letter := range alphabet { // add blocks for all letters
		c := cid.NewCidV0(u.Hash([]byte(letter)))
		prq.Push(l, &wantlist.Entry{Cid: c, Priority: math.MaxInt32 - index, Size: blockSize})
	}

	expectedAllocation := 100
	var out []string
	firstRound := true
	for {
		if !firstRound && prq.allocationForPeer(partner) == 0 {
			break
		}
		received := prq.Pop()
		firstRound = false
		expectedAllocation -= blockSize
		if prq.allocationForPeer(partner) != expectedAllocation {
			t.Fatalf("Expected allocation of %d, got %d", expectedAllocation, prq.allocationForPeer(partner))
		}
		out = append(out, received.Entries[0].Cid.String())
	}

	if prq.allocationForPeer(partner) != 0 {
		t.Fatalf("Peer should have 0 allocation, but has %d", prq.allocationForPeer(partner))
	}
	if len(out) != len(expectedOut) {
		t.Fatalf("Expected %d blocks popped, got %d", len(expectedOut), len(out))
	}
	for i, expected := range expectedOut {
		exp := cid.NewCidV0(u.Hash([]byte(expected))).String()
		if out[i] != exp {
			t.Fatalf("Expected %s, received %s", exp, out[i])
		}
	}
	if prq.partners[partner].taskQueue.Len() != len(expectedRemaining) {
		t.Fatalf("Expected %d blocks popped, got %d", len(expectedOut), len(out))
	}
	for _, expected := range expectedRemaining {
		cid := cid.NewCidV0(u.Hash([]byte(expected)))
		if _, ok := prq.taskMap[taskEntryKey{partner, cid}]; !ok {
			t.Fatalf("CID %s was not found in the peer's task map", cid)
		}
	}
}

// Multi-Peer Tests
// ----------------

func TestSPRQPushPop5Peers(t *testing.T) {
	roundBurst := 150
	prq := newSPRQ(&RRQConfig{RoundBurst: roundBurst, Strategy: Identity})
	partners := make([]peer.ID, 5)
	expectedAllocations := make(map[peer.ID]int)
	for i, _ := range partners {
		partners[i] = testutil.RandPeerIDFatal(t)
		expectedAllocations[partners[i]] = (i + 1) * 10
	}
	inputs := [5]string{"a", "ab", "abc", "abcd", "abcde"}

	blockSize := 10
	for i, letters := range inputs {
		l := newLedger(partners[i]).Receipt()
		l.Value = float64(i + 1)
		for j, letter := range strings.Split(letters, "") {
			c := cid.NewCidV0(u.Hash([]byte(letter)))
			prq.Push(l, &wantlist.Entry{Cid: c, Priority: math.MaxInt32 - j, Size: blockSize})
		}
	}

	numServes := 0
	for {
		received := prq.Pop()
		if received == nil {
			break
		}
		numServes += 1
		expectedAllocations[received.Target] -= blockSize
		if prq.allocationForPeer(received.Target) != expectedAllocations[received.Target] {
			t.Fatalf("Peer %s: Expected allocation of %d, got %d", received.Target.String(),
				expectedAllocations[received.Target], prq.allocationForPeer(received.Target))
		}
	}

	if numServes != 15 {
		t.Fatalf("Expected 15 serves, got %d", numServes)
	}
}

func TestSPRQSimpleStrategy(t *testing.T) {
	testStrategy(t, Identity)
}

func TestSPRQSigmoidStrategy(t *testing.T) {
	testStrategy(t, Sigmoid)
}

func TestSPRQTanhStrategy(t *testing.T) {
	testStrategy(t, Tanh)
}

func testStrategy(t *testing.T, strategy Strategy) {
	numPartners := 10
	blockSize := 1
	partners := make([]peer.ID, numPartners)
	ledgers := make([]*Receipt, numPartners)
	expectedAllocations := make(map[peer.ID]int)
	alphabet := strings.Split("abcdefghijklmnopqrstuvwxyz", "")

	// set up peer ledgers and calculate the total weight for the round robin round
	totalWeight := float64(0)
	for i := 0; i < numPartners; i += 1 {
		partners[i] = testutil.RandPeerIDFatal(t)
		ledgers[i] = newLedger(partners[i]).Receipt()
		ledgers[i].Value = float64(i)
		totalWeight += strategy(ledgers[i])
	}

	roundBurst := int(totalWeight)
	prq := newSPRQ(&RRQConfig{RoundBurst: roundBurst, Strategy: strategy})
	// calculate expected allocation for each peer and add blocks to queues
	for i, _ := range partners {
		expectedAllocations[partners[i]] = int(strategy(ledgers[i]) / totalWeight * float64(roundBurst))
		// add 26 blocks to each peer's queue
		for j := 0; j < len(alphabet); j += 1 {
			// add unique cid to peer's queue
			c := cid.NewCidV0(u.Hash([]byte(fmt.Sprintf("%s%d", alphabet[j], i))))
			prq.Push(ledgers[i], &wantlist.Entry{Cid: c, Priority: math.MaxInt32 - i - j, Size: blockSize})
		}
	}

	out := make(map[peer.ID][]string)
	// copy the expected allocations, as we'll need the original map later
	allocations := make(map[peer.ID]int)
	for id, allocation := range expectedAllocations {
		allocations[id] = allocation
	}

	// pop peers until there are no more blocks or the round ends
	for {
		received := prq.Pop()
		if received == nil {
			break
		}
		out[received.Target] = append(out[received.Target], received.Entries[0].Cid.String())
		// check whether round ended
		if prq.rrq.NumPeers() == 0 {
			break
		}
		allocations[received.Target] -= blockSize
		// check that allocation is as expected
		if prq.allocationForPeer(received.Target) != allocations[received.Target] {
			t.Fatalf("Peer %s: Expected allocation of %d, got %d", received.Target,
				allocations[received.Target], prq.allocationForPeer(received.Target))
		}
	}

	// check that the blocks popped in expected order for each peer
	for i, partner := range partners {
		numBlocks := min(expectedAllocations[partner], len(alphabet))
		if len(out[partner]) != numBlocks {
			t.Fatalf("Partner %s: Expected %d popped blocks, got %d", partner, numBlocks, len(out[partner]))
		}
		for j := 0; j < numBlocks; j += 1 {
			exp := cid.NewCidV0(u.Hash([]byte(fmt.Sprintf("%s%d", alphabet[j], i)))).String()
			if out[partner][j] != exp {
				t.Fatalf("Expected %s, received %s", exp, out[partner][j])
			}
		}
	}
}

// Helper Functions
// ----------------

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}
