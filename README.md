# tinydb
A tinydb is a small single-threaded database that supports read and write
structured data with limited external dependency.

# Storage Format
The data format is self-described. The layout is

```
-----------------------
|free space (4 bytes) |
-----------------------
| len(key)						|
-----------------------
| key									|
-----------------------
| len(column_value1)	|
-----------------------
| column_value1				|
-----------------------
| len(column_value2)	|
....
```

The first 4 bytes are the free space in the file. The default file size is 1KB.
After the free space size, the next entries are key length. The key length is
`varint`. The subsequent bytes are utf-8 byte. The system currently only
supports string value. After the key, it will be combination of column length
and column value itself. Currently, tinydb supports writing to a single 1KB-sized file.
Ideally, we should have:
1. Index in each file.
2. The ability to write to new file if there isn't enough space.
