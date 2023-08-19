package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/sirupsen/logrus"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db     *badger.DB
	conf   *config.Config
	logger *logrus.Logger
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	sas := StandAloneStorage{
		conf:   conf,
		logger: logrus.New(),
	}

	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	sas.db = db

	return &sas
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	r := standaloneStorageReader{tx: s.db.NewTransaction(false)}

	return &r, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	wb := new(engine_util.WriteBatch)
	for _, entry := range batch {
		wb.SetCF(entry.Cf(), entry.Key(), entry.Value())
	}

	return wb.WriteToDB(s.db)
}

type standaloneStorageReader struct {
	tx *badger.Txn
}

func (r *standaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(r.tx, cf, key)
}

func (r *standaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.tx)
}

func (r *standaloneStorageReader) Close() {
	r.tx.Discard()
}
