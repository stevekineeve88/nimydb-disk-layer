package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/managers"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/models"
	"os"
)

var dataLocation = "C:\\nimy-data-2"
var blobDiskManager = managers.CreateBlobDiskManager(dataLocation)
var dbDiskManager = managers.CreateDBDiskManager(dataLocation)
var formatDiskManager = managers.CreateFormatDiskManager(dataLocation)
var indexDiskManager = managers.CreateIndexDiskManager(dataLocation)
var pageDiskManager = managers.CreatePageDiskManager(dataLocation)
var partitionDiskManager = managers.CreatePartitionDiskManager(dataLocation)

func main() {
	db := "my_db"
	blob := "my_blob"
	format := models.Format{
		"col1": {
			KeyType: "int",
		},
		"col2": {
			KeyType: "string",
		},
	}
	records := models.PageRecords{}
	for i := 0; i < 1000; i++ {
		records[uuid.New().String()] = models.PageRecord{
			"col1": i % 3,
			"col2": "some random string",
		}
	}
	partition := models.Partition{Keys: []string{"col1"}}
	_ = dbDiskManager.Delete(db)
	createDB(db)
	createBlob(db, blob, format, partition)
	writeData(db, blob, records, partition)
}

func createDB(db string) {
	if err := dbDiskManager.Create(db); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func createBlob(db string, blob string, format models.Format, partition models.Partition) {
	if err := blobDiskManager.Create(db, blob); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	if err := formatDiskManager.Create(db, blob, format); err != nil {
		fmt.Println(err.Error())
		_ = blobDiskManager.Delete(db, blob)
		os.Exit(1)
	}
	if err := pageDiskManager.Initialize(db, blob); err != nil {
		fmt.Println(err.Error())
		_ = blobDiskManager.Delete(db, blob)
		os.Exit(1)
	}
	if err := indexDiskManager.Initialize(db, blob); err != nil {
		fmt.Println(err.Error())
		_ = blobDiskManager.Delete(db, blob)
		os.Exit(1)
	}
	if err := partitionDiskManager.Initialize(db, blob, partition); err != nil {
		fmt.Println(err.Error())
		_ = blobDiskManager.Delete(db, blob)
		os.Exit(1)
	}
}

func writeData(db string, blob string, records models.PageRecords, partition models.Partition) {
	partitionRecordMap := make(map[string]models.PageRecords)
	partitionPageMap := make(map[string]string)
	indexMap := make(map[string]models.IndexRecords)
	for key, record := range records {
		hashKey, _ := partitionDiskManager.GetHashKey(partition, record)
		if _, ok := partitionPageMap[hashKey]; !ok {
			page, _ := pageDiskManager.Create(db, blob)
			partitionRecordMap[hashKey] = models.PageRecords{}
			partitionPageMap[hashKey] = page
		}
		partitionRecordMap[hashKey][key] = record
		if _, ok := indexMap[indexDiskManager.GetPageRecordIdPrefix(key)]; !ok {
			indexMap[indexDiskManager.GetPageRecordIdPrefix(key)] = models.IndexRecords{}
		}
		indexMap[indexDiskManager.GetPageRecordIdPrefix(key)][key] = partitionPageMap[hashKey]
	}
	for hashKey, partitionRecords := range partitionRecordMap {
		page := partitionPageMap[hashKey]
		_ = pageDiskManager.WriteData(db, blob, page, partitionRecords)
		_ = partitionDiskManager.AddPage(db, blob, hashKey, page)
	}
	for prefix, indexRecords := range indexMap {
		indexFile, _ := indexDiskManager.Create(db, blob, prefix)
		_ = indexDiskManager.WriteData(db, blob, indexFile, indexRecords)
	}
}
