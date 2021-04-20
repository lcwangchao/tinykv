package standalone_storage

import (
	"bytes"
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db      *badger.DB
	opts    *badger.Options
	started bool
	stopped bool
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath

	return &StandAloneStorage{opts: &opts}
}

func (s *StandAloneStorage) Start() error {
	if s.started {
		return errors.New("Already started")
	}

	if s.stopped {
		return errors.New("Already closed")
	}

	db, err := badger.Open(*s.opts)
	if err != nil {
		return err
	}

	defer func() {
		if !s.started {
			db.Close()
		}
	}()

	s.db = db
	s.started = true
	return nil
}

func (s *StandAloneStorage) Stop() error {
	defer func() {}()

	if !s.started {
		return errors.New("Not started")
	}

	s.stopped = true

	if err := s.db.Close(); err != nil {
		return err
	}

	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &reader{txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			data := modify.Data
			var err error
			switch data.(type) {
			case storage.Put:
				err = s.putWithTxn(txn, data.(storage.Put))
			case storage.Delete:
				err = s.deleteWithTxn(txn, data.(storage.Delete))
			default:
				err = errors.New("Invalid modify")
			}

			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *StandAloneStorage) putWithTxn(txn *badger.Txn, put storage.Put) error {
	return txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
}

func (s *StandAloneStorage) deleteWithTxn(txn *badger.Txn, delete storage.Delete) error {
	return txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key))
}

// reader implements storage.StorageReader
type reader struct {
	txn *badger.Txn
}

func (r *reader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return value, err
}

func (r *reader) IterCF(cf string) engine_util.DBIterator {
	iterOptions := badger.DefaultIteratorOptions
	return &iterator{Iterator: r.txn.NewIterator(iterOptions), cf: cf}
}

func (r *reader) Close() {
	r.txn.Discard()
}

type iterator struct {
	*badger.Iterator
	cf string
}

func (i *iterator) Item() engine_util.DBItem {
	if !i.Valid() {
		return nil
	}

	return &item{DBItem: i.Iterator.Item(), cf: i.cf}
}

func (i *iterator) Valid() bool {
	if !i.Iterator.Valid() {
		return false
	}

	key := i.Iterator.Item().Key()
	return bytes.HasPrefix(key, []byte(i.cf))
}

func (i *iterator) Next() {
	i.Iterator.Next()
}

func (i *iterator) Seek(key []byte) {
	keyWithCf := engine_util.KeyWithCF(i.cf, key)
	i.Iterator.Seek(keyWithCf)
}

type item struct {
	engine_util.DBItem
	cf string
}

func (i *item) Key() []byte {
	return engine_util.TruncateCFFromKey(i.cf, i.DBItem.Key())
}

func (i *item) KeyCopy(dst []byte) []byte {
	return append(dst[:0], i.Key()...)
}
