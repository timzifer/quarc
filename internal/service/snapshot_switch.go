package service

import "sync/atomic"

type snapshotSwitch struct {
	buffers [2]map[string]*snapshotValue
	active  int32
}

func newSnapshotSwitch(size int) *snapshotSwitch {
	if size < 0 {
		size = 0
	}
	sw := &snapshotSwitch{}
	sw.buffers[0] = make(map[string]*snapshotValue, size)
	sw.buffers[1] = make(map[string]*snapshotValue, size)
	return sw
}

func (s *snapshotSwitch) Capture(store *cellStore) map[string]*snapshotValue {
	if s == nil || store == nil {
		return nil
	}
	current := atomic.LoadInt32(&s.active)
	next := int(current ^ 1)
	buf := store.snapshotInto(s.buffers[next])
	s.buffers[next] = buf
	atomic.StoreInt32(&s.active, int32(next))
	return buf
}

func (s *snapshotSwitch) Current() map[string]*snapshotValue {
	if s == nil {
		return nil
	}
	return s.buffers[atomic.LoadInt32(&s.active)]
}
