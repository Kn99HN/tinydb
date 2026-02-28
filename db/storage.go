package db

import (
	"os"
	"fmt"
	"log"
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"strconv"
)

const default_storage_write_mode = os.O_CREATE | os.O_RDWR
const permission = 0750
const default_file_size uint32 = 1024
const default_index_key_space_size = 3

type Column struct {
	name string
	col string
}

func (c Column) String() string {
	return fmt.Sprintf("%s:%s", c.name, c.col)
}

type Data struct {
	row_key string
	cols []Column
	size uint32
}

type Writer interface {
	Write(data *Data) bool
	WriteIndexFile(data *Data, free_space uint32) bool
	Flush() bool
}

type Reader interface {
	Read(s string) *Data
	Close() bool
}

type StorageWriter struct {
	file *os.File
	index_file *os.File
	index TreeNode
	use_index bool
}

type StorageReader struct {
	file *os.File
	index_file *os.File
	index TreeNode
	use_index bool
}

type DataIndex struct {
	min_key string
	max_key string
	file_path string
}

func initStorageReader(dir string, file_number int, use_index bool) *StorageReader {
	file_path := fmt.Sprintf("./%s/data_%d", dir, file_number)
	f, err := os.Open(file_path)
	if err != nil {
		panic("Failed to create a storage reader")
	}
	index_file_path := fmt.Sprintf("./%s/index_%d", dir, file_number)
	index_f, err := os.Open(index_file_path)
	if err != nil {
		panic("Failed to read index file for creating storage reader")
	}
	root := readIndexFile(index_f)
	return &StorageReader{ f, index_f, root, use_index }
}

func findOffset(r TreeNode, k string) string {
	v, _ := r.Find(k)
	// TODO: incorporate file name into reading
	splits := strings.Split(v, "-")
	return splits[1]
}

func (r *StorageReader) ReadRow(offset int64) (*Data, int64) {
	var free_space uint32 = 0
	file_size := make([]byte, 4)
	_, err := r.file.ReadAt(file_size, 0)
	if err == io.EOF { return nil, offset }
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(file_size); i++ {
		free_space = free_space << 8
		free_space |= uint32(file_size[i])
	}

	occupied_sz := default_file_size - free_space
	rd_offset := offset + 4
	if rd_offset >= int64(occupied_sz) {
		return nil, offset
	}
	_, err = r.file.Seek(rd_offset, 0)
	if err != nil {
		log.Fatal(err)
	}
	d := &Data{}
	d.cols = make([]Column, 0)
	payload_size, p_n, varint_err := parseVarInts(r.file)
	offset += int64(payload_size) + int64(p_n)
	if varint_err == io.EOF {
		return nil, offset
	}
	key_size, k_n, varint_err := parseVarInts(r.file)
	payload_size -= k_n
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
	for current_cols_size < max_cols_size {
		column_name_sz, cname_n, varint_err := parseVarInts(r.file)
		payload_size -= cname_n
		if varint_err == io.EOF { break }
		column_name, string_err := parseString(r.file, column_name_sz)
		if string_err == io.EOF { break }

		col_sz, c_n, varint_err := parseVarInts(r.file)
		payload_size -= c_n
		if varint_err == io.EOF { break }
		col_val, string_err := parseString(r.file, col_sz)
		if string_err == io.EOF { break }
		d.cols = append(d.cols, Column { column_name, col_val })
		current_cols_size += cname_n + len(column_name) + c_n + len(col_val)
	}
	d.size = uint32(payload_size)
	return d, offset
}


func (r *StorageReader) Read(s string) *Data {
	if r.index != nil {
		offset := findOffset(r.index, s)
		o, _ := strconv.Atoi(offset)
		d, _ := r.ReadRow(int64(o - 4))
		return d
	}
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
	d.cols = make([]Column, 0)
	for current_size < max_size_to_read {
		payload_size, p_n, varint_err := parseVarInts(r.file)
		current_size += uint32(payload_size) + uint32(p_n)
		if varint_err == io.EOF { break }
		key_size, k_n, varint_err := parseVarInts(r.file)
		if varint_err == io.EOF { break }
		key, string_err := parseString(r.file, key_size)
		if string_err == io.EOF { break }
		if key == s {
			max_cols_size := payload_size - (key_size + k_n)
			payload_size -= k_n
			current_cols_size := 0
			d.row_key = key
			for current_cols_size < max_cols_size {
				column_name_sz, cname_n, varint_err := parseVarInts(r.file)
				if varint_err == io.EOF { break }
				col_name, string_err := parseString(r.file, column_name_sz)
				if string_err == io.EOF { break }

				col_size, c_n, varint_err := parseVarInts(r.file)
				if varint_err == io.EOF { break }
				col_val, string_err := parseString(r.file, col_size)
				if string_err == io.EOF { break }
				d.cols = append(d.cols, Column { col_name, col_val })
				current_cols_size += cname_n + col_size + c_n + column_name_sz
				payload_size = payload_size - cname_n - c_n
			}
			d.size = uint32(payload_size)
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

func parseVarInts(f *os.File) (int, int, error) {
 continuation := true
 val := 0
 var err error = nil
 n := 0
 for continuation {
 	b := make([]byte, 1)
 	_, err = f.Read(b)
	if err != nil && err != io.EOF {
		return 0, 0, err
	}
	n++
 	val = val << 7
 	current_b := b[0]
 	continuation = ((current_b & 0x80) == 0x80)
 	val = val | int(current_b & 0x7F)
 }
 return val, n, err
}



func initStorageWriter(dir string, file_number int, use_index bool) *StorageWriter {
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
	index_file_name := fmt.Sprintf("./%s/index_%d", dir, file_number)
	index_file, err := os.OpenFile(index_file_name, default_storage_write_mode, permission)
	if err != nil {
		panic("Failed to read index file")
	}
	_, err = index_file.Write(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}
	err = index_file.Truncate(int64(default_file_size))
	if err != nil {
		log.Fatal(err)
	}
	return &StorageWriter{ f, index_file, nil, use_index }
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
	
	if s.use_index {
		index_written := s.writeIndexFile(p, offset)
		return index_written
	}
	return true
}

func (s *StorageWriter) writeIndexFile(p *Data, offset uint32) bool {
	if s.index == nil {
		s.index = readIndexFile(s.index_file)
	}
	s.index.Insert(p.row_key, fmt.Sprintf("%s-%d", s.file.Name(), offset))
	s.index_file.Seek(4, io.SeekStart)
	var sz uint32 = 0
	for n := range s.index.All() {
		for _, record := range n.GetIndexRecord() {
			sz += writeSingleIndexRecord(s, record, sz)
		}
	}
	index_free_space := default_file_size - (sz + 4)
	index_buf := new(bytes.Buffer)
	err := binary.Write(index_buf, binary.BigEndian, index_free_space)
	_, err = s.index_file.WriteAt(index_buf.Bytes(), 0)
	if err != nil {
		log.Fatal(err)
	}
	s.index_file.Seek(0, io.SeekStart)
	return true
}

func writeSingleIndexRecord(s *StorageWriter, record *IndexRecord,
	sz uint32) uint32 {
	data := ToBytesForString(fmt.Sprintf("%s,%s", (*record).k, (*record).v))
	if (sz + uint32(len(data))) > default_file_size {
		log.Fatal("No more space for index file")
	}
	_, err := s.index_file.Write(data)
	if err != nil {
			log.Fatal(err)
	}
	return uint32(len(data))
}

func readIndexFile(index_file *os.File) TreeNode {
	_, err := index_file.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}
	buf := make([]byte, 4)
	_, err = index_file.Read(buf)
	if err != nil {
		log.Fatal(err)
	}
	var free_space uint32 = 0
	for i := 0; i < len(buf); i++ {
		free_space = free_space << 8
		free_space |= uint32(buf[i])
	}
	max_size_to_read := int(default_file_size - free_space - 4)
	current_size := 0
	root := newRootNode(default_index_key_space_size)
	for current_size < max_size_to_read {
		payload_size, p_n, varint_err := parseVarInts(index_file)
		if varint_err == io.EOF { break }
		value, string_err := parseString(index_file, payload_size)
		if string_err == io.EOF { break }
		current_size += payload_size + p_n
		splits := strings.Split(value, ",")
		root.Insert(splits[0], splits[1])
	}
	_, err = index_file.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}
	return root
}

func ToBytesForString(p string) []byte {
	output := make([]byte, 0)
	payload_size := ToVarInts(len(p))
	output = append(output, payload_size...)
	for i := 0; i < len(p); i++{
		output = append(output, byte(p[i]))
	}
	return output
}

func ToBytes(p *Data) []byte {
	output := make([]byte, 0)
	key_size := len((*p).row_key)
	payload_sz := (*p).size
	key_length := ToVarInts(key_size)
	payload_sz += uint32(len(key_length))
	output = append(output, key_length...)
	for i := 0; i < key_size; i++{
		output = append(output, byte((*p).row_key[i]))
	}
	for _,v := range p.cols {
		name := v.name
		name_sz := ToVarInts(len(name))
		payload_sz += uint32(len(name_sz))
		output = append(output, name_sz...)
		for _, c := range name {
			output = append(output, byte(c))
		}

		col := v.col
		col_sz := ToVarInts(len(col))
		output = append(output, col_sz...)
		payload_sz += uint32(len(col_sz))
		for _, c := range col {
			output = append(output, byte(c))
		}
	}
	payload_length := ToVarInts(payload_sz)
	return append(payload_length, output...)
}

func (s *StorageWriter) Flush() {
	// flush any pending writes
	defer func() {
		if err := s.file.Close(); err != nil {
			log.Fatal(err)
		}
		if err := s.index_file.Close(); err != nil {
			log.Fatal(err)
		}
	}()
	if err := s.file.Sync(); err != nil {
		log.Fatal(err)
	}
	if err := s.index_file.Sync(); err != nil {
		log.Fatal(err)
	}
}
