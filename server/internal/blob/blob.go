// Package blob stores large event payloads by content hash. Event rows keep
// only a {"blob": "sha256:<hex>"} reference once a payload passes Threshold.
package blob

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Threshold is the payload size above which payloads move to the blob store.
const Threshold = 64 * 1024

type Store interface {
	Put(data []byte) (ref string, err error)
	Get(ref string) ([]byte, error)
}

// FS is a content-addressed blob store in a local folder (a shared volume in
// Docker Compose). MinIO/S3 can implement the same interface.
type FS struct{ Dir string }

func NewFS(dir string) (*FS, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FS{Dir: dir}, nil
}

func (f *FS) path(hexsum string) string {
	return filepath.Join(f.Dir, hexsum[:2], hexsum)
}

func (f *FS) Put(data []byte) (string, error) {
	sum := sha256.Sum256(data)
	h := hex.EncodeToString(sum[:])
	p := f.path(h)
	if _, err := os.Stat(p); err == nil {
		return "sha256:" + h, nil // content-addressed: already stored
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	tmp := p + ".tmp" + fmt.Sprint(os.Getpid())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		if _, statErr := os.Stat(p); statErr == nil {
			return "sha256:" + h, nil
		}
		return "", err
	}
	return "sha256:" + h, nil
}

func (f *FS) Get(ref string) ([]byte, error) {
	h, ok := strings.CutPrefix(ref, "sha256:")
	if !ok || len(h) != 64 {
		return nil, fmt.Errorf("bad blob ref %q", ref)
	}
	data, err := os.ReadFile(f.path(h))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != h {
		return nil, errors.New("blob checksum mismatch")
	}
	return data, nil
}

// Offload replaces a payload larger than Threshold with a blob reference.
func Offload(s Store, payload []byte) ([]byte, error) {
	if s == nil || len(payload) <= Threshold {
		return payload, nil
	}
	ref, err := s.Put(payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]string{"blob": ref})
}

// Resolve inlines a payload that is a blob reference.
func Resolve(s Store, payload []byte) ([]byte, error) {
	if s == nil || len(payload) > 200 || !bytes.Contains(payload, []byte(`"blob"`)) {
		return payload, nil
	}
	var ref map[string]string
	if err := json.Unmarshal(payload, &ref); err != nil || len(ref) != 1 || ref["blob"] == "" {
		return payload, nil
	}
	return s.Get(ref["blob"])
}
