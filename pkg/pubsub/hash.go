package pubsub

import (
	"errors"
	"hash/fnv"
	"sort"
)

type HashRing struct {
	ring map[uint32]string
	keys []uint32
}

func NewHashRing() *HashRing {
	return &HashRing{
		ring: make(map[uint32]string),
		keys: make([]uint32, 0),
	}
}

func (h *HashRing) Add(node string) {
	key := h.hash(node)

	if _, exists := h.ring[key]; exists {
		return
	}

	h.ring[key] = node
	h.keys = append(h.keys, key)

	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
	
}

func (h *HashRing) Remove(node string) {
	key := h.hash(node)

	if _, exists := h.ring[key]; !exists {
		return
	}

	delete(h.ring, key)

	for i, existingKey := range h.keys {
		if existingKey == key {
			h.keys = append(h.keys[:i], h.keys[i+1:]...)
			break
		}
	}
}


func (h *HashRing) Get(key string) (string, error) {

	if len(h.keys) == 0 {
		return "", errors.New("nenhum broker disponível no hash ring")
	}

	hash := h.hash(key)
	for _, ringKey := range h.keys {
		if hash <= ringKey {
			return h.ring[ringKey], nil
		}
	}

	return h.ring[h.keys[0]], nil
}

func (h *HashRing) hash(key string) uint32 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return hasher.Sum32()
}