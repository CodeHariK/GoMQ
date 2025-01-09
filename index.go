package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
)

type Index map[string]int64

type KVEntry struct {
	Key   string
	Value []byte
}

type Bitcask struct {
	activeFile   *os.File
	mmapData     []byte
	dataFiles    []*os.File
	index        map[string]FileOffset // In-memory index
	mu           sync.RWMutex
	maxFileSize  int
	activeOffset int
}

type FileOffset struct {
	FileID int   // Index of the file in the `dataFiles` slice; -1 for the active file
	Offset int64 // Byte offset within the file
}

type Record struct {
	Header Header
	Key    string
	Value  []byte
}

// Header represents the fixed width fields present at the start of every record.
type Header struct {
	Checksum  uint32
	Timestamp uint32
	Expiry    uint32
	KeySize   uint32
	ValSize   uint32
}

// Encode takes a byte buffer, encodes the value of header and writes to the buffer.
func (h *Header) encode(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, h)
}

// Decode takes a record object decodes the binary value the buffer.
func (h *Header) decode(record []byte) error {
	return binary.Read(bytes.NewReader(record), binary.LittleEndian, h)
}

func (db *Bitcask) initActiveFile(filename string, maxSize int) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}

	if err := file.Truncate(int64(maxSize)); err != nil {
		return err
	}

	mmapData, err := syscall.Mmap(int(file.Fd()), 0, maxSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	db.activeFile = file
	db.mmapData = mmapData
	db.maxFileSize = maxSize
	db.activeOffset = 0
	return nil
}

func (db *Bitcask) Put(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Prepare the record
	record := &Record{
		Header: Header{
			Checksum:  crc32.ChecksumIEEE(value), // Compute checksum for validation
			Timestamp: uint32(time.Now().Unix()), // Current timestamp
			Expiry:    0,                         // No expiry (optional feature)
			KeySize:   uint32(len(key)),
			ValSize:   uint32(len(value)),
		},
		Key:   key,
		Value: value,
	}

	// Encode the record
	encodedRecord, err := record.encode()
	if err != nil {
		return err
	}

	// Check for space in the active file
	if db.activeOffset+len(encodedRecord) > db.maxFileSize {
		if err := db.rotateActiveFile(); err != nil {
			return err
		}
	}

	// Write to mmap
	copy(db.mmapData[db.activeOffset:], encodedRecord)
	db.index[key] = FileOffset{FileID: -1, Offset: int64(db.activeOffset)}
	db.activeOffset += len(encodedRecord)

	return nil
}

func (db *Bitcask) Get(key string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	offset, ok := db.index[key]
	if !ok {
		return nil, fmt.Errorf("key not found")
	}

	var recordData []byte
	if offset.FileID == -1 {
		// Read from active mmap file
		recordData = db.mmapData[offset.Offset:]
	} else {
		// Read from an old file
		file := db.dataFiles[offset.FileID]
		file.Seek(offset.Offset, io.SeekStart)
		recordData = make([]byte, recordSize) // Ensure record size is calculated
		file.Read(recordData)
	}

	record, err := decodeRecord(recordData)
	if err != nil {
		return nil, err
	}

	return record.Value, nil
}

func (db *Bitcask) rotateActiveFile() error {
	// Unmap and close the current active file
	if err := syscall.Munmap(db.mmapData); err != nil {
		return err
	}
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// Rename the active file to an old file
	oldFileName := fmt.Sprintf("data-%d", len(db.dataFiles))
	if err := os.Rename("active.log", oldFileName); err != nil {
		return err
	}

	// Add the old file to the dataFiles list
	oldFile, err := os.Open(oldFileName)
	if err != nil {
		return err
	}
	db.dataFiles = append(db.dataFiles, oldFile)

	// Create and mmap a new active file
	return db.initActiveFile("active.log", db.maxFileSize)
}

func (db *Bitcask) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Create a temporary compacted file
	tempFile, err := os.Create("temp.log")
	if err != nil {
		return err
	}
	defer tempFile.Close()

	newIndex := make(Index)
	for key, offset := range db.index {
		var value []byte
		if offset.FileID == -1 {
			value, _ = decodeEntry(db.mmapData[offset.Offset:])
		} else {
			file := db.dataFiles[offset.FileID]
			file.Seek(offset.Offset, io.SeekStart)
			value, _ = decodeEntryFromReader(file)
		}
		entry := encodeEntry(key, value)
		newOffset := int64(tempFile.Seek(0, io.SeekEnd))
		tempFile.Write(entry)
		newIndex[key] = FileOffset{FileID: 0, Offset: newOffset}
	}

	// Replace old files with the compacted file
	os.Rename("temp.log", "data-0")
	db.dataFiles = []*os.File{tempFile}
	db.index = newIndex

	return nil
}

func (db *Bitcask) recoverIndex() error {
	// Load old files and rebuild the index
	for i, file := range db.dataFiles {
		offset := int64(0)
		for {
			entry, size, err := decodeEntryFromReader(file)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			db.index[entry.Key] = FileOffset{FileID: i, Offset: offset}
			offset += int64(size)
		}
	}

	// Load active file entries into the index
	offset := 0
	for offset < db.activeOffset {
		entry, size, err := decodeEntry(db.mmapData[offset:])
		if err != nil {
			return err
		}
		db.index[entry.Key] = FileOffset{FileID: -1, Offset: int64(offset)}
		offset += size
	}

	return nil
}

func decodeEntry(data []byte) ([]byte, error) {
	keySize := int32(binary.LittleEndian.Uint32(data[0:4]))
	valueSize := int32(binary.LittleEndian.Uint32(data[4:8]))
	value := data[8+keySize : 8+keySize+valueSize]
	return value, nil
}

func decodeEntryFromReader(r io.Reader) ([]byte, error) {
	var keySize, valueSize int32
	binary.Read(r, binary.LittleEndian, &keySize)
	binary.Read(r, binary.LittleEndian, &valueSize)
	r.Seek(int64(keySize), io.SeekCurrent) // Skip key
	value := make([]byte, valueSize)
	_, err := r.Read(value)
	return value, err
}
