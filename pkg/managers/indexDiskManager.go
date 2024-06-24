package managers

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/stevekineeve88/nimydb-disk-layer/pkg/models"
	"github.com/stevekineeve88/nimydb-disk-layer/pkg/utils"
	"sync"
)

const (
	indexesFile       = "indexes.json"
	indexesDirectory  = "indexes"
	indexPrefixLength = 1
)

type IndexDiskManager interface {
	Initialize(db string, blob string) error
	Create(db string, blob string, pageRecordId string) (string, error)
	GetAll(db string, blob string) (models.Indexes, error)
	GetData(db string, blob string, indexFileName string) (models.IndexRecords, error)
	WriteData(db string, blob string, indexFileName string, data models.IndexRecords) error
	Delete(db string, blob string, indexFileName string) (bool, error)
	GetPageRecordIdPrefix(pageRecordId string) string
}

type indexDiskManager struct {
	dataLocation string
}

var indexDiskManagerInstance *indexDiskManager

func CreateIndexDiskManager(dataLocation string) IndexDiskManager {
	sync.OnceFunc(func() {
		indexDiskManagerInstance = &indexDiskManager{dataLocation: dataLocation}
	})()
	return indexDiskManagerInstance
}

func (idm *indexDiskManager) Initialize(db string, blob string) error {
	indexesFilePath := idm.getIndexesFileName(db, blob)
	if err := utils.CreateFile(indexesFilePath); err != nil {
		return err
	}

	indexes := models.Indexes{}
	indexesData, _ := json.Marshal(indexes)
	if err := utils.WriteFile(indexesFilePath, indexesData); err != nil {
		return nil
	}

	return utils.CreateDir(idm.getIndexesDirectoryName(db, blob))
}

func (idm *indexDiskManager) Create(db string, blob string, pageRecordId string) (string, error) {
	newIndexFile := fmt.Sprintf("%s.json", uuid.New().String())
	newIndexFilePath := fmt.Sprintf("%s/%s", idm.getIndexesDirectoryName(db, blob), newIndexFile)
	if err := utils.CreateFile(newIndexFilePath); err != nil {
		return "", err
	}
	var indexRecords models.IndexRecords
	pageRecordsData, _ := json.Marshal(indexRecords)
	if err := utils.WriteFile(newIndexFilePath, pageRecordsData); err != nil {
		return newIndexFile, err
	}

	indexes, err := idm.GetAll(db, blob)
	if err != nil {
		return newIndexFile, err
	}

	indexPrefix := idm.GetPageRecordIdPrefix(pageRecordId)
	if indexItem, ok := indexes[indexPrefix]; !ok {
		indexes[indexPrefix] = models.IndexItem{FileNames: []string{newIndexFile}}
	} else {
		indexItem.FileNames = append(indexItem.FileNames, newIndexFile)
		indexes[indexPrefix] = indexItem
	}
	indexesData, _ := json.Marshal(indexes)
	err = utils.WriteFile(idm.getIndexesFileName(db, blob), indexesData)
	return newIndexFile, err
}

func (idm *indexDiskManager) GetAll(db string, blob string) (models.Indexes, error) {
	var indexes models.Indexes
	indexesFilePath := idm.getIndexesFileName(db, blob)
	file, err := utils.GetFile(indexesFilePath)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(file, &indexes)
	return indexes, err
}

func (idm *indexDiskManager) GetData(db string, blob string, indexFileName string) (models.IndexRecords, error) {
	var indexRecords models.IndexRecords
	file, err := utils.GetFile(fmt.Sprintf("%s/%s", idm.getIndexesDirectoryName(db, blob), indexFileName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &indexRecords)
	return indexRecords, err
}

func (idm *indexDiskManager) WriteData(db string, blob string, indexFileName string, data models.IndexRecords) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return utils.WriteFile(fmt.Sprintf("%s/%s", idm.getIndexesDirectoryName(db, blob), indexFileName), dataBytes)
}

func (idm *indexDiskManager) Delete(db string, blob string, indexFileName string) (bool, error) {
	err := utils.DeleteFile(fmt.Sprintf("%s/%s", idm.getIndexesDirectoryName(db, blob), indexFileName))
	if err != nil {
		return false, err
	}

	indexes, err := idm.GetAll(db, blob)
	if err != nil {
		return true, err
	}
	for prefix, _ := range indexes {
		for i := 0; i < len(indexes[prefix].FileNames); i++ {
			if indexes[prefix].FileNames[i] == indexFileName {
				filesNames := indexes[prefix].FileNames
				copy(filesNames[i:], filesNames[i+1:])
				filesNames[len(filesNames)-1] = ""
				filesNames = filesNames[:len(filesNames)-1]
				indexes[prefix] = models.IndexItem{FileNames: filesNames}
				indexesData, _ := json.Marshal(indexes)
				err = utils.WriteFile(idm.getIndexesFileName(db, blob), indexesData)
				if err != nil {
					return true, err
				}
				return true, nil
			}
		}
	}
	return true, nil
}

func (idm *indexDiskManager) GetPageRecordIdPrefix(pageRecordId string) string {
	return pageRecordId[0:indexPrefixLength]
}

func (idm *indexDiskManager) getIndexesFileName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", idm.dataLocation, db, blob, indexesFile)
}

func (idm *indexDiskManager) getIndexesDirectoryName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", idm.dataLocation, db, blob, indexesDirectory)
}
