package pkg

import (
	"hash/fnv"
	"sort"
)

type HashRing struct {
	ring map[uint32]string
	keys []uint32
}

func New() *HashRing {
	return &HashRing{ring: make(map[uint32]string)}
}

func (h *HashRing) Add(node string) {
	key := h.hash(node)
	h.ring[key] = node
	h.keys = append(h.keys, key)
	sort.Slice(h.keys, func(i, j int) bool { return h.keys[i] < h.keys[j] })
}

func (h *HashRing) Get(key string) string {
	hash := h.hash(key)
	for _, k := range h.keys {
		if hash <= k {
			return h.ring[k]
		}
	}
	return h.ring[h.keys[0]]
}

func (h *HashRing) hash(key string) uint32 {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return hasher.Sum32()
}