package raft_boltdb

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	raft "github.com/fuyao-w/go-raft"
	"os"
	"testing"
	"time"
)

func TestKVStore(t *testing.T) {
	filePath := "test.db"
	got, err := NewStore(filePath, bolt.DefaultOptions, true)
	if err != nil {
		t.Errorf("NewStore() error = %v", err)
		return
	}
	defer os.Remove(filePath)

	if err = got.Set("age", "15"); err != nil {
		t.Fatal(err)
	}
	t.Log(got.Get("age"))

	if err = got.SetUint64("height", 15); err != nil {
		t.Fatal(err)
	}
	t.Log(got.GetUint64("height"))
	t.Log(got.Sync())

	t.Log(got.Get("sss"))
	t.Log(got.GetUint64("sss"))

}

func TestLogStore(t *testing.T) {
	print := func(i any) string {
		data, _ := json.Marshal(i)
		return string(data)
	}
	filePath := "test.db"
	store, err := NewStore(filePath, bolt.DefaultOptions, true)
	if err != nil {
		t.Errorf("NewStore() error = %v", err)
		return
	}
	defer os.Remove(filePath)
	t.Log(store.SetLogs([]*raft.LogEntry{
		{
			Type:      1,
			Index:     1,
			Term:      1,
			Data:      []byte(`{"age" : 15}`),
			DataV2:    "",
			CreatedAt: time.Now(),
		},
	}))
	log, err := store.GetLog(1)
	t.Log(print(log), err)

	t.Log(store.SetLogs([]*raft.LogEntry{
		{
			Type:      1,
			Index:     2,
			Term:      1,
			Data:      []byte(`{"age" : 16}`),
			DataV2:    "",
			CreatedAt: time.Now(),
		},
	}))
	logs, err := store.GetLogRange(1, 2)
	t.Log(print(logs), err)

	store.DeleteRange(1, 2)
	logs, err = store.GetLogRange(1, 2)
	t.Log(print(logs), err)
	t.Log(store.GetLog(199))
}
