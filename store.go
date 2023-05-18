package raft_boltdb

import (
	"errors"
	"github.com/boltdb/bolt"

	raft "github.com/fuyao-w/papillon"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	dbMod = 0600
)

var (
	bucketLogs    = []byte("logs")
	bucketKV      = []byte("kv")
	ErrNotFound   = errors.New("not found")
	ErrKeyIsNil   = errors.New("key is nil")
	ErrValueIsNil = errors.New("value is nil")
	ErrRange      = errors.New("from must no bigger than to")
)

type Store struct {
	filePath string
	noSync   bool
	db       *bolt.DB
}

func NewStore(filePath string, boltOptions *bolt.Options, noSync bool) (*Store, error) {
	db, err := bolt.Open(filePath, dbMod, boltOptions)
	if err != nil {
		return nil, err
	}
	db.NoSync = noSync
	store := &Store{
		filePath: filePath,
		noSync:   noSync,
		db:       db,
	}
	if !boltOptions.ReadOnly {
		if err = store.init(); err != nil {
			return nil, err
		}
	}
	return store, err
}
func (s *Store) init() error {
	return s.db.Update(func(tx *bolt.Tx) (err error) {
		if _, err = tx.CreateBucketIfNotExists(bucketLogs); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketKV); err != nil {
			return err
		}
		return
	})
}

func (s *Store) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return nil, ErrKeyIsNil
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketKV)
		v := bucket.Get(key)
		if len(v) == 0 {
			return ErrNotFound
		}
		val = append([]byte(nil), v...)
		return nil
	})
	return

}

func (s *Store) Set(key []byte, val []byte) (err error) {
	if len(key) == 0 {
		return ErrKeyIsNil
	}
	if len(val) == 0 {
		return ErrValueIsNil
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketKV)
		return bucket.Put(key, val)
	})
}

func (s *Store) SetUint64(key []byte, val uint64) error {
	if len(key) == 0 {
		return ErrKeyIsNil
	}
	return s.Set(key, uint2Bytes(val))
}

func (s *Store) GetUint64(key []byte) (val uint64, err error) {
	if len(key) == 0 {
		return 0, ErrKeyIsNil
	}
	v, err := s.Get(key)
	if err != nil {
		return
	}
	return bytes2Uint(v), nil
}

func (s *Store) FirstIndex() (res uint64, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketLogs).Cursor()
		if first, _ := cursor.First(); len(first) != 0 {
			res = parseLogKey(first)
		}
		return nil
	})
	return
}

func (s *Store) LastIndex() (res uint64, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketLogs).Cursor()
		if first, _ := cursor.Last(); len(first) != 0 {
			res = parseLogKey(first)
		}
		return nil
	})
	return
}

func (s *Store) GetLog(index uint64) (log *raft.LogEntry, err error) {
	//defer func(begin int64) {
	//	fmt.Println("get log cost :", time.Now().UnixMilli()-begin, index)
	//}(time.Now().UnixMilli())
	//time.Sleep(time.Millisecond * 20)
	err = s.db.View(func(tx *bolt.Tx) error {
		result := tx.Bucket(bucketLogs).Get(buildLogKey(index))
		if len(result) == 0 {
			return ErrNotFound
		}
		_ = msgpack.Unmarshal(result, &log)
		return nil
	})
	return
}

func (s *Store) GetLogRange(from, to uint64) (logs []*raft.LogEntry, err error) {
	if from > to {
		return nil, ErrRange
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketLogs).Cursor()
		fromKey := buildLogKey(from)
		for key, val := cursor.Seek(fromKey); len(key) > 0; key, val = cursor.Next() {
			if len(key) == 0 {
				break
			}
			if parseLogKey(key) <= to {
				var log raft.LogEntry
				_ = msgpack.Unmarshal(val, &log)
				logs = append(logs, &log)
			}
		}
		return nil
	})
	return
}

func (s *Store) SetLogs(logs []*raft.LogEntry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, log := range logs {
			key := buildLogKey(log.Index)
			val, err := msgpack.Marshal(log)
			if err != nil {
				return err
			}
			bucket := tx.Bucket(bucketLogs)

			if err = bucket.Put(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) DeleteRange(from, to uint64) error {
	if from > to {
		return ErrRange
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(bucketLogs).Cursor()
		for key, _ := cursor.Seek(buildLogKey(from)); key != nil; key, _ = cursor.Next() {
			if parseLogKey(key) > to {
				break
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) Sync() error {
	return s.db.Sync()
}

func (s *Store) Close() error {
	return s.db.Close()
}
