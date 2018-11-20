package tool

import (
	"sort"
	"sync"
)

type Set struct {
	l map[string]struct{}
	m *sync.RWMutex
}

func NewSet(threadSafe bool) *Set {
	var m *sync.RWMutex
	if threadSafe {
		m = new(sync.RWMutex)
	}

	return &Set{
		l: make(map[string]struct{}),
		m: m,
	}
}

func (s *Set) Add(name string) {
	if s.m != nil {
		s.m.Lock()
		defer s.m.Unlock()
	}

	s.l[name] = struct{}{}
}

func (s *Set) Contains(name string) bool {
	if s.m != nil {
		s.m.RLock()
		defer s.m.RUnlock()
	}

	_, ok := s.l[name]
	return ok
}

func (s *Set) List() []string {
	l := make([]string, len(s.l))

	if s.m != nil {
		s.m.RLock()
	}

	var i int
	for k := range s.l {
		l[i] = k
		i++
	}

	if s.m != nil {
		s.m.RUnlock()
	}

	sort.Strings(l)
	return l
}
