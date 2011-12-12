package mry

import (
	"os"
	mysql "github.com/Philio/GoMySQL"
)
// FIXME: cross storage transaction?
// TODO: MySQL pooling


type Storage interface {
	Init()
	//CreateTable(table string, nbKey int)
	//CreateIndex(table, column string)
	GetTransaction() (StorageTransaction, os.Error)
}

type StorageTransaction interface {
	Get(table string, keys []string) ([]byte, os.Error)
	Set(table string, keys []string, data []byte) os.Error
	Commit() os.Error
}

func compoundKeyString(table string, keys ...string) string {
	ret := table + "_"
	for _, key := range keys {
		ret += "_" + key
	}
	return ret
}

// In-memory storage
type MemoryStorage struct {
	data map[string][]byte
}

func (ms *MemoryStorage) Init() {
	ms.data = make(map[string][]byte)
}

func (ms *MemoryStorage) GetTransaction() (StorageTransaction, os.Error) {
	return &MemoryStorageTransaction{
		storage:  ms,
		tempData: make(map[string][]byte),
	}, nil
}

type MemoryStorageTransaction struct {
	storage  *MemoryStorage
	tempData map[string][]byte
}

func (mst *MemoryStorageTransaction) Get(table string, keys []string) ([]byte, os.Error) {
	// TODO: keep track of data version so we can ensure transaction atomicity

	if val, found := mst.tempData[compoundKeyString(table, keys...)]; found {
		return val, nil
	}

	if val, found := mst.storage.data[compoundKeyString(table, keys...)]; found {
		return val, nil
	}

	return nil, nil
}

func (mst *MemoryStorageTransaction) Set(table string, keys []string, data []byte) os.Error {
	mst.tempData[compoundKeyString(table, keys...)] = data
	return nil
}

func (mst *MemoryStorageTransaction) Commit() os.Error {
	// TODO: check current versions for atomicity
	for key, data := range mst.tempData {
		mst.storage.data[key] = data
	}

	return nil
}


// MySQL storage
type MysqlStorage struct {
	Host	  string
	Username  string
	Password  string
	Database  string
}

func (ms *MysqlStorage) Init() {
	// TODO: create pool
}

func (ms *MysqlStorage) getClient() (*mysql.Client, os.Error) {
	// TODO: pooling!
	return mysql.DialTCP(ms.Host, ms.Username, ms.Password, ms.Database)
}

func (ms *MysqlStorage) GetTransaction() (StorageTransaction, os.Error) {
	client, err := ms.getClient()
	if err != nil {
		return nil, err
	}
	
	// start transaction
	client.Start()

	return &MysqlStorageTransaction{
		storage: ms,
		client: client,
	}, nil
}

type MysqlStorageTransaction struct {
	storage  *MysqlStorage
	client   *mysql.Client
}

func (t *MysqlStorageTransaction) Get(table string, keys []string) ([]byte, os.Error) {
	stmt, err := t.client.Prepare("SELECT d FROM `" + t.client.Escape(table) + "` WHERE k = ?")
	if err != nil {
		return nil, err
	}

	err = stmt.BindParams(keys[0])
	if err != nil {
		return nil, err
	}

	err = stmt.Execute()	
	if err != nil {
		return nil, err
	}

	var data []byte 
	stmt.BindResult(&data)

	eof, err := stmt.Fetch()
	if eof || err != nil {
		return nil, err
	}


	err = stmt.FreeResult()
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (t *MysqlStorageTransaction) Set(table string, keys []string, data []byte) os.Error {
	stmt, err := t.client.Prepare("INSERT INTO `" + t.client.Escape(table) + "` (k,d) VALUES (?,?) ON DUPLICATE KEY UPDATE d=VALUES(d)")
	if err != nil {
		return err
	}

	err = stmt.BindParams(keys[0], data)
	if err != nil {
		return err
	}

	err = stmt.Execute()	
	if err != nil {
		return err
	}

	return nil
}

func (t *MysqlStorageTransaction) Commit() os.Error {
	return t.client.Commit()
}


