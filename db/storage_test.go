package db

import (
	"testing"
	"fmt"
)

func TestWrite(t *testing.T) {
	wr := initStorageWriter("test")
	row_key := "Dune"
	cols := []string{"2020", "Dennis"}
	size := uint32(len(row_key) + len(cols))
	expected_data := &Data{row_key, cols, size}
	succeeded := wr.Write(expected_data)
	if !succeeded {
		t.Errorf("Failed to write data")
	}

	r := initStorageReader("test")

	actual_data := r.Read(row_key)
	fmt.Sprintf("Actual data %v\n", actual_data)
	
	if expected_data != actual_data {
		t.Errorf("Expected %v. Actual %v", expected_data, actual_data)
	}
}

