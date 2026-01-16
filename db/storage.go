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
const default_storage_write_mode = os.O_APPEND | os.O_CREATE | os.O_RDWR
const permission = 0750
const default_file_size uint32 = 1024

type Data struct {
	row_key string
	cols []string
	size uint32
}

type Writer interface {
	Write(data *Data) bool
	Flush() bool
}

type Reader interface {
	Read(s string) *Data
}

type StorageWriter struct {
	i int
	dir string
	file *os.File
}

type StorageReader struct {
	file *os.File
}

type DataIndex struct {
	min_key string
	max_key string
	file_path string
}

func initStorageReader(dir string) *StorageReader {
	// a single file for now
	file_path := fmt.Sprintf("data_0")
	f, err := os.Open(file_path)
	if err != nil {
		panic("Failed to create a storage reader")
	}
	return &StorageReader{ f }
}

func (r StorageReader) Read(s string) *Data {
	var offset int64 = 4
	_, err := r.file.Seek(offset, 0)
	if err != nil {
		log.Fatal(err)
	}
	d := &Data{}
	d.cols = make([]string, 0)
	for true {
		// these function should return error
		payload_length, varint_err := parseVarInts(r.file)
		if varint_err == io.EOF { break }
		key_length, varint_err := parseVarInts(r.file)
		if varint_err == io.EOF { break }
		key, string_err := parseString(r.file, key_length)
		if string_err == io.EOF { break }
		if key == s {
			cols_length := payload_length - key_length
			d.row_key = key
			d.size = uint32(payload_length)
			for cols_length != 0 {
				col_length, varint_err := parseVarInts(r.file)
				if varint_err == io.EOF { break }
				col_val, string_err := parseString(r.file, col_length)
				if string_err == io.EOF { break }
				cols_length = cols_length - col_length
				d.cols = append(d.cols,col_val)
			}
		}
	}
	if d.size == 0 { return nil }
	return d
}

func parseString(f *os.File, str_length int) (string, error) {
	result := make([]byte, str_length)
	var err error = nil
	for i := 0; i < str_length; i++ {
		b := make([]byte, 1)
		_, err = f.Read(b)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		result[i] = b[0]
	}
	return string(result), err
}

func parseVarInts(f *os.File) (int, error) {
 continuation := true
 val := 0
 var err error = nil
 for continuation {
 	b := make([]byte, 1)
 	_, err = f.Read(b)
	if err != nil && err != io.EOF {
		return 0, err
	}
 	val = val << 7
 	current_b := b[0]
 	continuation = ((current_b & 0x80) == 0x80)
 	val = val | int(current_b & 0x7F)
 }
 return val, err
}



func initStorageWriter(dir string) *StorageWriter {
	file_path := fmt.Sprintf("./data_%d", 0)
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
	return &StorageWriter{ 0, dir, f }
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
	n, err := s.file.WriteAt(data, int64(offset))
	fmt.Printf("Write %d bytes of data", n)
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
	file_path := fmt.Sprintf("%s/data_%d", s.dir, s.i)
	old_file := s.file
	f, err := os.OpenFile(file_path, default_storage_write_mode, permission)
	s.file = f
	if err != nil {
		panic("Failed to create a new file for storage writer")
	}
	old_file.Close()
}
