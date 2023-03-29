package raft_boltdb

import (
	"encoding/binary"
	"strconv"

	. "github.com/fuyao-w/common-util"
)

func uint2Bytes(i uint64) []byte {
	return strconv.AppendUint([]byte(nil), i, 10)
}
func bytes2Uint(b []byte) uint64 {
	res, _ := strconv.ParseUint(Bytes2Str(b), 10, 64)
	return res
}

// buildLogKey
func buildLogKey(ts uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, ts)
	return out
}

// ParseLogKey parses the index from the key bytes.
func parseLogKey(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(key)
}
