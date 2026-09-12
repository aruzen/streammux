package streammux

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestPeerConfigRejectsInvalidInboundQueuePolicy(t *testing.T) {
	config := DefaultPeerConfig()
	config.InboundQueuePolicy = InboundQueuePolicy(2)
	if _, err := config.withDefaults(); err == nil {
		t.Fatal("invalid inbound queue policy was accepted")
	}
}

func TestReservePendingWrapsCorrelationIDWithoutZero(t *testing.T) {
	p := &Peer{pending: make(map[CorrelationID]*peerPending), canceled: make(map[CorrelationID]peerPending)}
	p.next.Store(math.MaxUint64 - 1)
	first, err := p.reservePending()
	if err != nil || first != CorrelationID(math.MaxUint64) {
		t.Fatalf("first correlation = %d, %v", first, err)
	}
	second, err := p.reservePending()
	if err != nil || second != 1 {
		t.Fatalf("wrapped correlation = %d, %v", second, err)
	}
}

func TestPeerSendCopiesAndSendOwnedTransfersPayload(t *testing.T) {
	queuedPayload := func(owned bool) ([]byte, []byte) {
		config := DefaultPeerConfig()
		p := &Peer{config: config, outbound: make(map[StreamID]*outboundStream), changed: make(chan struct{}), done: make(chan struct{})}
		payload := []byte("original")
		frame, err := NewOwnedFrame(Header{Version: 1, MessageType: 7, Flags: FlagEvent, StreamID: 1}, payload)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			if owned {
				result <- p.SendOwned(ctx, frame, TrafficBulk)
			} else {
				result <- p.Send(ctx, frame, TrafficBulk)
			}
		}()
		deadline := time.Now().Add(time.Second)
		for {
			p.mu.Lock()
			state := p.outbound[1]
			if state != nil && len(state.queue) == 1 {
				queued := state.queue[0].frame.Payload
				p.mu.Unlock()
				payload[0] = 'X'
				cancel()
				<-result
				return queued, payload
			}
			p.mu.Unlock()
			if time.Now().After(deadline) {
				t.Fatal("frame was not queued")
			}
			time.Sleep(time.Millisecond)
		}
	}
	copied, _ := queuedPayload(false)
	if string(copied) != "original" {
		t.Fatalf("Send queued aliased payload %q", copied)
	}
	transferred, mutated := queuedPayload(true)
	if &transferred[0] != &mutated[0] {
		t.Fatal("SendOwned copied transferred payload")
	}
}

func TestLateResponseRemovesCanceledOrderTombstone(t *testing.T) {
	p := &Peer{
		config:        PeerConfig{MaxCanceledRequests: 1},
		pending:       make(map[CorrelationID]*peerPending),
		canceled:      map[CorrelationID]peerPending{1: {messageType: 9, streamID: 2}},
		canceledOrder: []CorrelationID{1},
	}
	frame := Frame{Header: Header{MessageType: 9, Flags: FlagResponse, CorrelationID: 1, StreamID: 2}}
	if err := p.resolve(frame); err != nil {
		t.Fatal(err)
	}
	p.pending[1] = &peerPending{messageType: 9, streamID: 2, response: make(chan peerResult, 1)}
	p.abandonPending(1)
	if _, ok := p.canceled[1]; !ok {
		t.Fatal("stale canceled order evicted a reused CorrelationID")
	}
}

func TestWeightedSelectionPrioritizesControlWithoutStarvingBulk(t *testing.T) {
	config := DefaultPeerConfig()
	p := &Peer{config: config, outbound: make(map[StreamID]*outboundStream), changed: make(chan struct{})}
	add := func(id StreamID, class TrafficClass) {
		request := &outboundRequest{class: class, bytes: 1}
		p.outbound[id] = &outboundStream{id: id, queue: []*outboundRequest{request}, bytes: 1}
		p.outboundOrder = append(p.outboundOrder, id)
		p.outboundBytes++
		p.outboundFrames++
	}
	var id StreamID
	for range 10 {
		id++
		add(id, TrafficControl)
	}
	for range 6 {
		id++
		add(id, TrafficInteractive)
	}
	for range 3 {
		id++
		add(id, TrafficBulk)
	}

	classes := []TrafficClass{TrafficControl, TrafficInteractive, TrafficBulk}
	weights := map[TrafficClass]int{TrafficControl: config.ControlWeight, TrafficInteractive: config.InteractiveWeight, TrafficBulk: config.BulkWeight}
	classIndex, credit, cursor := 0, 0, 0
	got := make([]TrafficClass, 0, 13)
	for len(got) < cap(got) {
		class := classes[classIndex]
		if credit == 0 {
			credit = weights[class]
		}
		request, next := p.takeLocked(class, cursor)
		cursor = next
		if request != nil {
			got = append(got, request.class)
			credit--
		}
		if request == nil || credit == 0 {
			classIndex = (classIndex + 1) % len(classes)
			credit = 0
		}
	}
	for index, class := range got {
		want := TrafficControl
		if index >= 8 && index < 12 {
			want = TrafficInteractive
		} else if index == 12 {
			want = TrafficBulk
		}
		if class != want {
			t.Fatalf("selection[%d] = %d, want %d; all=%v", index, class, want, got)
		}
	}
}
