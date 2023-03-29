package raft_boltdb

import (
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
