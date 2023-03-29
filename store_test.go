package raft_boltdb

import (
	"github.com/boltdb/bolt"
	raft "github.com/fuyao-w/go-raft"
	"os"
	"testing"
	"time"

	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"reflect"
)

func TestKVStore(t *testing.T) {
	path := "test"
	store, err := NewStore(path, bolt.DefaultOptions, true)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	Convey("set unit", t, func() {
		So(store.SetUint64([]byte("age"), 15), ShouldBeNil)
		val, err := store.GetUint64([]byte("age"))
		So(err, ShouldBeNil)
		So(val, ShouldEqual, 15)

	})
	Convey("set log", t, func() {
		log := &raft.LogEntry{
			Type:      1,
			Index:     100,
			Term:      1,
			Data:      []byte("abc"),
			CreatedAt: time.Now(),
		}
		So(store.SetLogs([]*raft.LogEntry{log}), ShouldBeNil)
		logResp, err := store.GetLog(100)
		So(err, ShouldBeNil)
		So(deepEqual(log, logResp), ShouldBeTrue)
	})

}

func deepEqual(a, b *raft.LogEntry) bool {
	return a.Term == b.Term && a.Index == b.Index && reflect.DeepEqual(a.Data, b.Data) && a.CreatedAt.Unix() == b.CreatedAt.Unix()
}

func TestLogRange(t *testing.T) {
	defer os.Remove("test")
	type testCase struct {
		From, To uint64
	}
	var testList = []testCase{
		{
			1, 99,
		},
		{
			15, 99,
		},
		{
			15, 110,
		},
	}
	Convey("get range", t, func() {
		for i, tc := range testList {
			Convey(fmt.Sprintf("case %d", i), func() {
				store, err := NewStore("test", bolt.DefaultOptions, true)
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				var logs []*raft.LogEntry
				for i := tc.From; i <= tc.To; i++ {
					logs = append(logs, &raft.LogEntry{
						Type:  1,
						Index: i,
						Term:  1,
						Data:  nil,
					})
				}
				So(store.SetLogs(logs), ShouldBeNil)

				idx, err := store.FirstIndex()

				So(err, ShouldBeNil)
				So(idx, ShouldEqual, tc.From)

				idx, err = store.LastIndex()
				So(err, ShouldBeNil)
				So(idx, ShouldEqual, tc.To)

				logs, err = store.GetLogRange(tc.From, tc.To)
				So(err, ShouldBeNil)
				So(len(logs), ShouldEqual, tc.To-tc.From+1)

				for i := tc.From; i <= tc.To; i++ {
					log, err := store.GetLog(i)
					So(err, ShouldBeNil)
					So(log.Index, ShouldEqual, i)
				}

				store.DeleteRange(tc.From, tc.To)
				for i := tc.From; i <= tc.To; i++ {
					_, err := store.GetLog(i)
					So(err, ShouldEqual, ErrKeyNotFound)
				}
			})

		}

	})
}
