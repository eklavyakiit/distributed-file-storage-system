package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/eklavyakiit/distributed-file-storage-system/p2p"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Store struct {
	dir string
	mu  sync.RWMutex
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func validHash(h string) bool {
	b, e := hex.DecodeString(h)
	return e == nil && len(b) == 32 && h == strings.ToLower(h)
}
func NewStore(dir string) (*Store, error) {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	return &Store{dir: dir}, nil
}
func (s *Store) Put(h string, b []byte) error {
	if len(b) > p2p.MaxFile || !validHash(h) || digest(b) != h {
		return errors.New("invalid content")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, h)
	if old, e := os.ReadFile(path); e == nil && digest(old) == h {
		return nil
	}
	f, e := os.CreateTemp(s.dir, ".tmp-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return e
	}
	if ce != nil {
		return ce
	}
	return os.Rename(f.Name(), path)
}
func (s *Store) Get(h string) ([]byte, error) {
	if !validHash(h) {
		return nil, errors.New("invalid hash")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, e := os.ReadFile(filepath.Join(s.dir, h))
	if e == nil && digest(b) != h {
		return nil, errors.New("corrupt content")
	}
	return b, e
}
func (s *Store) List() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries, e := os.ReadDir(s.dir)
	if e != nil {
		return nil, e
	}
	out := []string{}
	for _, f := range entries {
		if f.Type().IsRegular() && validHash(f.Name()) {
			out = append(out, f.Name())
		}
	}
	return out, nil
}
