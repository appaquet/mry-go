package mry

import (
	"fmt"
	mysql "github.com/gnanderson/GoMySQL"
	"strconv"
	"strings"
	"time"
)

// TODO: Use another library to suport exp/sql
// TODO: Pooling

// MySQL storage
type MysqlStorage struct {
	Host     string
	Username string
	Password string
	Database string
}

func (m *MysqlStorage) Init() {
	// TODO: create pool
}

func (m *MysqlStorage) getClient() (*mysql.Client, error) {
	// TODO: pooling!

	if strings.HasPrefix(m.Host, "/") {
		return mysql.DialUnix(m.Host, m.Username, m.Password, m.Database)
	}

	return mysql.DialTCP(m.Host, m.Username, m.Password, m.Database)
}

func (m *MysqlStorage) GetTransaction(trxTime time.Time) (StorageTransaction, error) {
	client, err := m.getClient()
	if err != nil {
		return nil, err
	}

	// start transaction
	client.Start()

	return &MysqlStorageTransaction{
		trxTime: trxTime,
		storage: m,
		client:  client,
	}, nil
}

func (m *MysqlStorage) CreateTable(table string, depth int) error {
	trx, err := m.GetTransaction(time.Now())
	if err != nil {
		return err
	}

	mTrx := trx.(*MysqlStorageTransaction)

	sql := "CREATE TABLE `" + mTrx.client.Escape(table) + "` ("
	sql = sql + "	`t` bigint(20) NOT NULL AUTO_INCREMENT,"

	kList := ""
	for i := 1; i <= depth; i++ {
		sql = sql + "	`k" + strconv.Itoa(i) + "` varchar(128) NOT NULL,"
		if kList != "" {
			kList = kList + ","
		}
		kList = kList + "k" + strconv.Itoa(i)
	}

	sql = sql + "	`d` longtext NOT NULL,"
	sql = sql + "	PRIMARY KEY (`t`," + kList + ")"
	sql = sql + ") ENGINE=InnoDB  DEFAULT CHARSET=utf8;"

	err = mTrx.client.Query(sql)
	if err != nil {
		return err
	}

	err = mTrx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *MysqlStorage) Nuke() error {
	trx, err := m.GetTransaction(time.Now())
	if err != nil {
		return err
	}

	mTrx := trx.(*MysqlStorageTransaction)

	err = mTrx.client.Query("DROP DATABASE " + mTrx.client.Escape(m.Database))
	if err != nil {
		return err
	}

	err = mTrx.client.Query("CREATE DATABASE " + mTrx.client.Escape(m.Database))
	if err != nil {
		return err
	}

	err = mTrx.Commit()
	if err != nil {
		return err
	}

	return nil
}

type MysqlStorageTransaction struct {
	trxTime time.Time
	storage *MysqlStorage
	client  *mysql.Client
}

func (t *MysqlStorageTransaction) buildBinding(row *Row, nbKeys int) []interface{} {
	switch nbKeys {
		case 1:
			return []interface{}{&row.IntTimestamp, &row.Key1, &row.Data}
		case 2:
			return []interface{}{&row.IntTimestamp, &row.Key1, &row.Key2, &row.Data}
		case 3:
			return []interface{}{&row.IntTimestamp, &row.Key1, &row.Key2, &row.Key3, &row.Data}
		case 4:
			return []interface{}{&row.IntTimestamp, &row.Key1, &row.Key2, &row.Key3, &row.Key4, &row.Data}
	}

	panic("Unsuported number of keys")
}

func (t *MysqlStorageTransaction) Get(table string, keys []string) (*Row, error) {
	sqlKeys := ""
	for i := 1; i <= len(keys); i++ {
		if sqlKeys != "" {
			sqlKeys += " AND "
		}
		sqlKeys += fmt.Sprintf("k%d = ?", i)
	}

	curKey := fmt.Sprintf("k%d", len(keys))
	stmt, err := t.client.Prepare("SELECT t,"+curKey+",d FROM `" + t.client.Escape(table) + "` WHERE " + sqlKeys + " AND `t` <= ? ORDER BY `t` DESC LIMIT 0,1")
	if err != nil {
		return nil, err
	}

	iKeys := make([]interface{}, len(keys)+1)
	for i, key := range keys {
		iKeys[i] = key
	}
	iKeys[len(keys)] = t.trxTime.UnixNano()

	err = stmt.BindParams(iKeys...)
	if err != nil {
		return nil, err
	}

	err = stmt.Execute()
	if err != nil {
		return nil, err
	}

	row := &Row{}
	bindings := t.buildBinding(row, len(keys))
	err = stmt.BindResult(bindings...)
	if err != nil {
		return nil, err
	}

	eof, err := stmt.Fetch()
	if eof || err != nil {
		return nil, err
	}

	err = stmt.FreeResult()
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (t *MysqlStorageTransaction) GetQuery(query StorageQuery) (RowIterator, error) {
	// TODO: support query filters, limit, etc.

	// prepare query
	table := t.client.Escape(query.Table)
	sql :=      "SELECT top.*"
	sql = sql + " FROM `"+table+"` AS top"
	sql = sql + " WHERE top.t = ("
	sql = sql + "   SELECT MAX(alt.t)"
	sql = sql + "   FROM `"+table+"` AS alt"
	sql = sql + "   WHERE " 

	groupKeys := ""
	for i:=1; i <= len(query.TablePrefix)+1; i++ {
		if i >= 2 {
			sql = sql + " AND "
			groupKeys = groupKeys + ", "
		}
		groupKeys = groupKeys + " alt.k" + strconv.Itoa(i)
		sql = sql + " alt.k"+strconv.Itoa(i)+" = top.k"+strconv.Itoa(i)
	}

	sql = sql + "   GROUP BY " + groupKeys
	sql = sql + " )"

	stmt, err := t.client.Prepare(sql)
	if err != nil {
		return nil, err
	}

	err = stmt.Execute()
	if err != nil {
		return nil, err
	}

	iterator := &mysqlRowIterator{&Row{}, stmt}
	bindings := t.buildBinding(iterator.row, len(query.TablePrefix)+1)
	err = stmt.BindResult(bindings...)
	if err != nil {
		return nil, err
	}

	return iterator, nil
}

func (t *MysqlStorageTransaction) Set(table string, keys []string, data []byte) error {
	sqlKeys := ""
	sqlUpdateKeys := ""
	sqlValues := ""
	for i := 1; i <= len(keys); i++ {
		if sqlKeys != "" {
			sqlKeys += ","
			sqlValues += ","
			sqlUpdateKeys += " AND "
		}
		sqlKeys += fmt.Sprintf("k%d", i)
		sqlUpdateKeys += fmt.Sprintf("k%d = ?", i)
		sqlValues += "?"
	}

	stmt, err := t.client.Prepare("INSERT INTO `" + t.client.Escape(table) + "` (`t`, " + sqlKeys + ",d) VALUES (?," + sqlValues + ",?) ON DUPLICATE KEY UPDATE d=VALUES(d)")
	if err != nil {
		return err
	}

	iKeys := make([]interface{}, len(keys)+2)
	iKeys[0] = t.trxTime.UnixNano()
	for i, key := range keys {
		iKeys[i+1] = key
	}
	iKeys[len(iKeys)-1] = data

	err = stmt.BindParams(iKeys...)
	if err != nil {
		return err
	}

	err = stmt.Execute()
	if err != nil {
		return err
	}

	return nil
}

func (t *MysqlStorageTransaction) GetTimeline(table string, nbKey int, from time.Time, count int) ([][2]*Row, error) {
	sql := ""
	sql = sql + "	SELECT new.*, old.*"
	sql = sql + "	FROM `" + t.client.Escape(table) + "` AS new "
	sql = sql + "	LEFT JOIN `" + t.client.Escape(table) + "` AS old ON ("

	for i:=1; i<=nbKey; i++ {
		sql = sql + "new.k"+strconv.Itoa(i)+" = old.k"+strconv.Itoa(i)+" AND "
	}

	sql = sql + "        old.t < new.t ) "
	sql = sql + "	WHERE old.t IS NULL OR old.t = ("
	sql = sql + "		SELECT MAX(alt.t)"
	sql = sql + "		FROM `" + t.client.Escape(table) + "` AS alt"
	sql = sql + "		WHERE alt.t < new.t "

	groupKeys := ""
	for i:=1; i<=nbKey; i++ {
		sql = sql + "   AND alt.k"+strconv.Itoa(i)+" = old.k"+strconv.Itoa(i)

		if i >= 2 {
			groupKeys = groupKeys + ", "
		}
		groupKeys  = groupKeys + "alt.k"+strconv.Itoa(i)
	}

	sql = sql + "		GROUP BY " + groupKeys
	sql = sql + "	)"
	sql = sql + fmt.Sprintf("   AND new.t >= %d", from.UnixNano())
	sql = sql + "	ORDER BY new.t ASC"
	sql = sql + "	LIMIT 0, " + strconv.Itoa(count)

	stmt, err := t.client.Prepare(sql)
	if err != nil {
		return nil, err
	}

	err = stmt.Execute()
	if err != nil {
		return nil, err
	}

	oldRow := &Row{}
	newRow := &Row{}

	bindings1 := t.buildBinding(oldRow, nbKey)
	bindings2 := t.buildBinding(newRow, nbKey)
	bindings1 = append(bindings1, bindings2...)

	err = stmt.BindResult(bindings1...)
	if err != nil {
		return nil, err
	}

	ret := make([][2]*Row, 0)
	for {
		eof, _ := stmt.Fetch()
		if eof {
			break
		}

		ret = append(ret, [2]*Row{
			{oldRow.IntTimestamp, oldRow.Timestamp, oldRow.Key1, oldRow.Key2, oldRow.Key3, oldRow.Key4, oldRow.Data},
			{newRow.IntTimestamp, newRow.Timestamp, newRow.Key1, newRow.Key2, newRow.Key3, newRow.Key4, newRow.Data},
		})

		oldRow.Reset()
		newRow.Reset()
	}

	return ret, nil
}

func (t *MysqlStorageTransaction) Rollback() error {
	err := t.client.Rollback()
	t.client.Close()
	return err
}

func (t *MysqlStorageTransaction) Commit() error {
	err := t.client.Commit()
	t.client.Close()
	return err
}

// RowIterator for MySQL
type mysqlRowIterator struct {
	row  *Row
	stmt *mysql.Statement
}

func (i *mysqlRowIterator) Next() (*Row, error) {
	eof, err := i.stmt.Fetch()
	if err != nil {
		return nil, err
	}

	if eof {
		return nil, nil
	}

	return i.row, nil
}

func (i *mysqlRowIterator) Close() {
	_ = i.stmt.FreeResult()
	// TODO: do something with error?
}
