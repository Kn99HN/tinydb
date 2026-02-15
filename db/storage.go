package db

import (
	"os"
	"fmt"
	"log"
	"bytes"
	"encoding/binary"
	"io"
)

const default_storage_write_mode = os.O_CREATE | os.O_RDWR
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
	Close() bool
}

type StorageWriter struct {
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

func initStorageReader(dir string, file_number int) *StorageReader {
	file_path := fmt.Sprintf("./%s/data_%d", dir, file_number)
	f, err := os.Open(file_path)
	if err != nil {
		panic("Failed to create a storage reader")
	}
	return &StorageReader{ f }
}

func (r *StorageReader) ReadRow(offset int64) (*Data, int64) {
	_, err := r.file.Seek(offset + 4, 0)
	if err != nil {
		log.Fatal(err)
	}
	d := &Data{}
	d.cols = make([]string, 0)
	payload_size, varint_err := parseVarInts(r.file)
	offset += int64(payload_size)
	if varint_err == io.EOF {
		return nil, offset
	}
	key_size, varint_err := parseVarInts(r.file)
	if varint_err == io.EOF { 
		return nil, offset
	}
	key, string_err := parseString(r.file, key_size)
	if string_err == io.EOF {
		return nil, offset
	}
	max_cols_size := payload_size - key_size
	current_cols_size := 0
	d.row_key = key
	d.size = uint32(payload_size)
	for current_cols_size < max_cols_size {
		col_size, varint_err := parseVarInts(r.file)
		current_cols_size += col_size
		if varint_err == io.EOF { break }
		col_val, string_err := parseString(r.file, col_size)
		if string_err == io.EOF { break }
		d.cols = append(d.cols, col_val)
	}
	offset += int64(max_cols_size)
	return d, offset
}


func (r *StorageReader) Read(s string) *Data {
	_, err := r.file.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}
	buf := make([]byte, 4)
	_, err = r.file.Read(buf)
	if err != nil {
		log.Fatal(err)
	}
	var free_space uint32 = 0
	for i := 0; i < len(buf); i++ {
		free_space = free_space << 8
		free_space |= uint32(buf[i])
	}
	max_size_to_read := default_file_size - free_space
	var current_size uint32 = 4
	d := &Data{}
	d.cols = make([]string, 0)
	for current_size < max_size_to_read {
		payload_size, varint_err := parseVarInts(r.file)
		current_size += uint32(payload_size)
		if varint_err == io.EOF { break }
		key_size, varint_err := parseVarInts(r.file)
		if varint_err == io.EOF { break }
		key, string_err := parseString(r.file, key_size)
		if string_err == io.EOF { break }
		if key == s {
			max_cols_size := payload_size - key_size
			current_cols_size := 0
			d.row_key = key
			d.size = uint32(payload_size)
			for current_cols_size < max_cols_size {
				col_size, varint_err := parseVarInts(r.file)
				current_cols_size += col_size
				if varint_err == io.EOF { break }
				col_val, string_err := parseString(r.file, col_size)
				if string_err == io.EOF { break }
				d.cols = append(d.cols, col_val)
			}
		} else {
			if _, err := r.file.Seek(int64(current_size), 0); err != nil {
				log.Fatal(err)
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



func initStorageWriter(dir string, file_number int) *StorageWriter {
	file_path := fmt.Sprintf("./%s/data_%d", dir, file_number)
	f, err := os.OpenFile(file_path, default_storage_write_mode, permission)
	if err != nil {
		panic("Failed to create a storage writer")
	}
	buf := new(bytes.Buffer)
	fsize := default_file_size - 4
	err = binary.Write(buf, binary.BigEndian, fsize)
	if err != nil {
		log.Fatal(err)
	}
	_, err = f.Write(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	err = f.Truncate(int64(default_file_size))
	if err != nil {
		log.Fatal(err)
	}
	return &StorageWriter{ f }
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
	_, err := s.file.ReadAt(file_size, 0)
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
	offset := default_file_size - fsize
	data := ToBytes(p)
	_, err = s.file.WriteAt(data, int64(offset))
	if err != nil {
		log.Fatal(err)
	}
	buf := new(bytes.Buffer)
	new_file_size := fsize - uint32(len(data))
	err = binary.Write(buf, binary.BigEndian, new_file_size)
	_, err = s.file.WriteAt(buf.Bytes(), 0)
	if err != nil {
		log.Fatal(err)
	}
	return true
}

func ToBytes(p *Data) []byte {
	output := make([]byte, 0)
	key_size := len((*p).row_key)
	payload_length := ToVarInts((*p).size)
	output = append(output, payload_length...)
	key_length := ToVarInts(key_size)
	output = append(output, key_length...)
	for i := 0; i < key_size; i++{
		output = append(output, byte((*p).row_key[i]))
	}
	for _,v := range p.cols {
		col_length := ToVarInts(len(v))
		output = append(output, col_length...)
		for _, c := range v {
			output = append(output, byte(c))
		}
	}
	return output
}

func (s *StorageWriter) Flush() {
	// flush any pending writes
	defer func() {
		if err := s.file.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	if err := s.file.Sync(); err != nil {
		log.Fatal(err)
	}
}
