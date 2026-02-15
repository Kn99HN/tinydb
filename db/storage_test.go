package db

import (
	"testing"
	"fmt"
	"log"
	"reflect"
	"os"
	"math/rand"
	"time"
)

const dir = "./test"
const test_dir_permission = 0750
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func TestMain(m *testing.M) {
	fmt.Printf("Creating %s directory for testing data\n", dir)

	err := os.Mkdir(dir, test_dir_permission)
	if err != nil && !os.IsExist(err) {
		log.Fatal(fmt.Sprintf("Failed to setup %s directory for testing", dir))
	}

	code := m.Run()

	fmt.Printf("Performing dir cleanup\n")
	if err := os.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}

	os.Exit(code)
}

func generateRandomData(st_size int, cols_length int) *Data {
	rand.Seed(time.Now().UnixNano())
	b := make([]rune, st_size)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	key := string(b)
	cols := make([]string, cols_length)
	for j := 0; j < cols_length; j++ {
		col := make([]rune, st_size)
		for i := range col {
			col[i] = letters[rand.Intn(len(letters))]
		}
		cols[j] = string(col)
	}
	return &Data{ key, cols, uint32(st_size + st_size * cols_length) }
}

func TestWrite(t *testing.T) {
	file_number := 0
	wr := initStorageWriter(dir, file_number)
	expected_data := generateRandomData(2, 2)
	succeeded := wr.Write(expected_data)
	if !succeeded {
		t.Errorf("Failed to write data")
	}
	wr.Flush()

	r := initStorageReader(dir, file_number)
	actual_data := r.Read(expected_data.row_key)
	if !reflect.DeepEqual(expected_data, actual_data) {
		t.Errorf("Expected %v. Actual %v", expected_data, actual_data)
	}
}

func TestWriteMultipleRecordsReadSingleRecord(t *testing.T) {
	file_number := 0
	wr := initStorageWriter(dir, file_number)
	d1 := generateRandomData(2, 2)
	d2 := generateRandomData(2, 2)
	succeeded := wr.Write(d1)
	if !succeeded {
		t.Errorf("Failed to write data")
	}
	succeeded = wr.Write(d2)
	if !succeeded {
		t.Errorf("Failed to write data")
	}

	wr.Flush()

	r := initStorageReader(dir, file_number)
	actual_data := r.Read(d1.row_key)
	if !reflect.DeepEqual(d1, actual_data) {
		t.Errorf("Expected %v. Actual %v", d1, actual_data)
	}
}

func TestWriteMultipleRecordsReadRows(t *testing.T) {
	file_number := 0
	wr := initStorageWriter(dir, file_number)
	d1 := generateRandomData(2, 2)
	d2 := generateRandomData(2, 2)
	succeeded := wr.Write(d1)
	if !succeeded {
		t.Errorf("Failed to write data")
	}
	succeeded = wr.Write(d2)
	if !succeeded {
		t.Errorf("Failed to write data")
	}

	wr.Flush()

	r := initStorageReader(dir, file_number)
	r1, offset_1 := r.ReadRow(0)
	if !reflect.DeepEqual(d1, r1) {
		t.Errorf("Expected %v. Actual %v", d1, r1)
	}

	r2, _ := r.ReadRow(offset_1)
	if !reflect.DeepEqual(d2, r2) {
		t.Errorf("Expected %v. Actual %v", d2, r2)
	}
}

