package db

import (
	"os"
	"fmt"
	"log"
	"bytes"
	"encoding/binary"
	"io"
)

const dir = "blocks"
const default_storage_write_mode = os.O_APPEND | os.O_CREATE | os.O_WRONLY
const permission = 0644
const default_file_size uint32 = 1024

type Data struct {
	row_key string
	cols []string
	size uint32
}

type Writer interface {
	Write(data Data) bool
	Flush() bool
}

type Reader interface {
	ReadIndex(fpath string) []DataIndex
}

type StorageWriter struct {
	i int
	file *os.File
}

type StorageReader struct {}

type DataIndex struct {
	min_key string
	max_key string
	file_path string
}


func initStorageWriter(dir string) *StorageWriter {
	file_path := fmt.Sprintf("%s/data_%d", dir, 0)
	f, err := os.OpenFile(file_path, default_storage_write_mode, permission)
	if err != nil {
		panic("Failed to create a storage writer")
	}
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.BigEndian, default_file_size)
	if err != nil {
		log.Fatal(err)
	}
	_, err = f.Write(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	return &StorageWriter{ 0, f }
}

func parseVarInts(buffer []byte, starting_index int) (int, int) {
 continuation := true
 val := 0
 for continuation {
 	val = val << 7
 	b := buffer[starting_index]
 	continuation = ((b & 0x80) == 0x80)
 	if starting_index == len(buffer) && continuation {
 	 panic("Reached the end of byte stream but there are still more bytes to decode")
 	}
 	starting_index += 1
 	val = val | int(b & 0x7F)
 }
 return val, starting_index
}

func ToVarInts [T ~uint32| ~uint64 | ~int32 | ~int64 | ~int] (i T) []byte {
	buf := make([]byte, 0)
	if i == 0 {
		return []byte{0}
	}
	for i != 0 {
		b := byte(i & 0x7F)
		if len(buf) >= 1 {
	 		b = b | 0x80
		}
		buf = append([]byte{b}, buf...)
		i = i >> 7
	}
	return buf
}


/**
Data file_format:

file_offset = file_size - current file size

[ current file size (4 bytes) ]
[ len(record) ]
[ len(key) ]
[ key ]
[ len(column) ]
[ column ]
[ ....]

index file format:

[ current_file_size ]
[ len(min_key) ]
[ min_key ]
[ len(max_key) ]
[ max_key ]
[ len(file_name) ]
[ file_name ]


**/
func (s *StorageWriter) Write(p *Data) bool {
	/*
	1. (Advanced) Read the index file -> binary search based on the key. This will return a
	set of files for overlapping ranges. Choose first one that fits
	2. Read the size of the file -> no more size, go to next until we have to
	create a new file
	3. Write the row record into the last offset.
	4. Return true if the file accepts the writes
	*/
	var fsize uint32 = 0
	file_size := make([]byte, 4)
	_, err := s.file.Read(file_size)
	if err == io.EOF { return false }
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(file_size); i++ {
		fsize = fsize << 8
		fsize |= uint32(file_size[i])
	}
	if p.size > fsize {
		fmt.Printf("Data size is %d compared to available size %d", p.size, fsize)
		return false
	}
	offset := uint32(len(file_size)) + (default_file_size - fsize)
	data := ToBytes(p)
	_, err = s.file.WriteAt(data, int64(offset))
	if err != nil {
		log.Fatal(err)
	}
	buf := new(bytes.Buffer)
	new_file_size := fsize + p.size
	err = binary.Write(buf, binary.BigEndian, new_file_size)
	_, err = s.file.WriteAt(buf.Bytes(), 0)
	if err != nil {
		log.Fatal(err)
	}
	return true
}

func ToBytes(p *Data) []byte {
	output := make([]byte, 0)
	key_length := ToVarInts(len((*p).row_key))
	for _,v := range(key_length) {
		output = append(output, v)
	}
	for _,v := range(p.cols) {
		value_length := ToVarInts(len(v))
		output = append(output, value_length...)
	}
	return output
}

func (s *StorageWriter) Flush() {
	// flush any pending writes
	s.i += 1
	file_path := fmt.Sprintf("%s/data_%d", dir, s.i)
	old_file := s.file
	f, err := os.OpenFile(file_path, default_storage_write_mode, permission)
	s.file = f
	if err != nil {
		panic("Failed to create a new file for storage writer")
	}
	old_file.Close()
}
