package mry

import (
	"testing"
	"time"
)

func getStorage(t *testing.T, nuke bool) Storage {
	s := &MysqlStorage{
		Host:     "localhost",
		Username: "mry_test",
		Password: "mry_test",
		Database: "mry_test",
	}

	if nuke {
		err := s.Nuke()
		if err != nil {
			t.Fatalf("Couldn't nuke database: %s", err)
		}
	}

	return s
}

func TestGetSet(t *testing.T) {
	s := getStorage(t, true)

	model := newModel()
	model.CreateTable("getset")
	err := s.SyncModel(model)
	if err != nil {
		t.Fatal(err)
	}

	trx, err := s.GetTransaction(time.Now())
	defer trx.Commit()

	if err != nil {
		t.Fatal(err)
	}

	err = trx.Set("getset", []string{"key1"}, []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}

	row, err := trx.Get("getset", []string{"key1"})
	if err != nil {
		t.Fatal(err)
	}

	if row == nil || string(row.Data) != "value1" {
		t.Fatalf("Got different value than set: %s!=value1", row)
	}

	err = trx.Set("getset", []string{"key1"}, []byte("value2"))
	if err != nil {
		t.Fatal(err)
	}

	row, err = trx.Get("getset", []string{"key1"})
	if err != nil {
		t.Fatal(err)
	}

	if row == nil || string(row.Data) != "value2" {
		t.Fatalf("Got different value than set: %s!=value2", row)
	}

}

func TestGetSetRollback(t *testing.T) {
	s := getStorage(t, false)

	model := newModel()
	model.CreateTable("getsetrollback")
	err := s.SyncModel(model)
	if err != nil {
		t.Fatal(err)
	}

	trx, err := s.GetTransaction(time.Now())
	defer trx.Rollback()

	if err != nil {
		t.Fatal(err)
	}

	err = trx.Set("getsetrollback", []string{"key1"}, []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}

	err = trx.Rollback()
	if err != nil {
		t.Fatal(err)
	}

	trx, err = s.GetTransaction(time.Now())
	defer trx.Rollback()

	row, err := trx.Get("getsetrollback", []string{"key1"})
	if err != nil {
		t.Fatal(err)
	}

	if row != nil {
		t.Fatalf("Row shoulnd't exist after a rollback", row)
	}
}

func TestGetSetIsolation(t *testing.T) {
	s := getStorage(t, false)

	model := newModel()
	model.CreateTable("getsetisolation")
	err := s.SyncModel(model)
	if err != nil {
		t.Fatal(err)
	}

	trx, err := s.GetTransaction(time.Now())
	defer trx.Commit()

	if err != nil {
		t.Fatal(err)
	}

	err = trx.Set("getsetisolation", []string{"key1"}, []byte("value1"))
	if err != nil {
		t.Fatal(err)
	}

	trx.Commit()

	trx, err = s.GetTransaction(time.Now().Add(time.Duration(100000)))
	defer trx.Commit()

	if err != nil {
		t.Fatal(err)
	}

	err = trx.Set("getsetisolation", []string{"key1"}, []byte("value2"))
	if err != nil {
		t.Fatal(err)
	}

	row, err := trx.Get("getsetisolation", []string{"key1"})
	if err != nil {
		t.Fatal(err)
	}

	if row == nil || string(row.Data) != "value2" {
		t.Fatalf("Didn't receive expected value: %s!=value2", row)
	}

	err = trx.Set("getsetisolation", []string{"key1"}, []byte("value3"))
	if err != nil {
		t.Fatal(err)
	}

	row, err = trx.Get("getsetisolation", []string{"key1"})
	if err != nil {
		t.Fatal(err)
	}

	if row == nil || string(row.Data) != "value3" {
		t.Fatalf("Didn't receive expected value: %s!=value2", row)
	}
}

func TestQuery(t *testing.T) {
	s := getStorage(t, false)

	model := newModel()
	model.CreateTable("query")
	err := s.SyncModel(model)
	if err != nil {
		t.Fatal(err)
	}

	trx, _ := s.GetTransaction(time.Now())
	defer trx.Commit()
	trx.Set("query", []string{"key0"}, []byte("0value1"))
	trx.Set("query", []string{"key1"}, []byte("1value1"))
	trx.Set("query", []string{"key2"}, []byte("2value2"))
	trx.Set("query", []string{"key3"}, []byte("3value1"))
	trx.Commit()

	trx, _ = s.GetTransaction(time.Now().Add(1))
	defer trx.Commit()
	trx.Set("query", []string{"key1"}, []byte("1value2"))
	trx.Set("query", []string{"key2"}, []byte("2value2"))
	trx.Commit()

	trx, _ = s.GetTransaction(time.Now().Add(2))
	defer trx.Commit()
	trx.Set("query", []string{"key1"}, []byte("1value3"))
	trx.Commit()

	trx, _ = s.GetTransaction(time.Now().Add(3))
	defer trx.Commit()

	iter, err := trx.GetQuery(StorageQuery{
		Table: "query",
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		row, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}

		if row != nil {
			if row.Key1 == "key0" && string(row.Data) != "0value1" {
				t.Fatalf("Expected 0value1, got %s", row.Data)
			}

			if row.Key1 == "key1" && string(row.Data) != "1value3" {
				t.Fatalf("Expected 1value3, got %s", row.Data)
			}

			if row.Key1 == "key2" && string(row.Data) != "2value2" {
				t.Fatalf("Expected 2value2, got %s", row.Data)
			}

			if row.Key1 == "key3" && string(row.Data) != "3value1" {
				t.Fatalf("Expected 3value1, got %s", row.Data)
			}

		}
	}
}

func TestTimeline(t *testing.T) {
	s := getStorage(t, false)

	model := newModel()
	model.CreateTable("timeline")
	err := s.SyncModel(model)
	if err != nil {
		t.Fatal(err)
	}

	trx, _ := s.GetTransaction(time.Now())
	defer trx.Commit()
	trx.Set("timeline", []string{"key0"}, []byte("0value1"))
	trx.Set("timeline", []string{"key1"}, []byte("1value1"))
	trx.Set("timeline", []string{"key2"}, []byte("2value2"))
	trx.Set("timeline", []string{"key3"}, []byte("3value1"))
	trx.Commit()

	trx, _ = s.GetTransaction(time.Now().Add(1))
	defer trx.Commit()
	trx.Set("timeline", []string{"key1"}, []byte("1value2"))
	trx.Set("timeline", []string{"key2"}, []byte("2value2"))
	trx.Commit()

	trx, _ = s.GetTransaction(time.Now().Add(2))
	defer trx.Commit()
	trx.Set("timeline", []string{"key1"}, []byte("1value3"))
	trx.Set("timeline", []string{"key4"}, []byte("4value1"))
	trx.Commit()

	trx, _ = s.GetTransaction(time.Now().Add(3))
	defer trx.Commit()

	changes, err := trx.GetTimeline("timeline", 1, time.Unix(0, 0), 100)
	if err != nil {
		t.Fatal(err)
	}

	for _, mut := range changes {
		first, sec := mut.OldRow, mut.NewRow

		if first.Key1 == "key0" && string(first.Data) == "0value1" && sec.Data != nil {
			t.Fatalf("Got an 'old' value, expected null: %s - %s", *first, *sec)
		}
		if first.Key1 == "key1" && string(first.Data) == "1value2" && string(sec.Data) != "1value1" {
			t.Fatalf("Got invalid old value, expected 1value1: %s - %s", *first, *sec)
		}
		if first.Key1 == "key4" && (string(first.Data) != "4value1" || sec.Data != nil) {
			t.Fatalf("Got invalid old value, expected 4value1: %s - %s", *first, *sec)
		}
	}
}
