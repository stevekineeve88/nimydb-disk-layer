package managers

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/models"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/utils"
	"sync"
)

const (
	partitionsFile      = "partitions.json"
	partitionsDirectory = "partitions"
)

type PartitionDiskManager interface {
	Initialize(db string, blob string, partition models.Partition) error
	AddPage(db string, blob string, hashKeyFileName string, pageFileName string) error
	GetPartition(db string, blob string) (models.Partition, error)
	GetByHashKey(db string, blob string, hashKeyFileName string) (models.PartitionPages, error)
	GetAll(db string, blob string) ([]string, error)
	Remove(db string, blob string, hashKeyFileName string, pageFileName string) error
	Delete(db string, blob string, hashKeyFileName string) error
	GetHashKey(partition models.Partition, pageRecord models.PageRecord) (string, error)
}

type partitionDiskManager struct {
	dataLocation string
}

var partitionDiskManagerInstance *partitionDiskManager

func CreatePartitionDiskManager(dataLocation string) PartitionDiskManager {
	sync.OnceFunc(func() {
		partitionDiskManagerInstance = &partitionDiskManager{dataLocation: dataLocation}
	})()
	return partitionDiskManagerInstance
}

func (pdm *partitionDiskManager) Initialize(db string, blob string, partition models.Partition) error {
	partitionFilePath := pdm.getPartitionsFileName(db, blob)
	err := utils.CreateFile(partitionFilePath)
	if err != nil {
		return err
	}

	partitionData, err := json.Marshal(partition)
	if err != nil {
		return err
	}
	err = utils.WriteFile(partitionFilePath, partitionData)
	if err != nil {
		return err
	}

	return utils.CreateDir(pdm.getPartitionsDirectoryName(db, blob))
}

func (pdm *partitionDiskManager) AddPage(db string, blob string, hashKeyFileName string, pageFileName string) error {
	partitionPages, err := pdm.GetByHashKey(db, blob, hashKeyFileName)
	if err != nil {
		partitionPages, err = pdm.createHashKey(db, blob, hashKeyFileName)
		if err != nil {
			return err
		}
	}
	for _, partitionPage := range partitionPages {
		if partitionPage.FileName == pageFileName {
			return nil
		}
	}

	partitionPages = append(partitionPages, models.PartitionPageItem{FileName: pageFileName})
	partitionPagesData, err := json.Marshal(partitionPages)
	if err != nil {
		return err
	}

	return utils.WriteFile(fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName), partitionPagesData)
}

func (pdm *partitionDiskManager) GetPartition(db string, blob string) (models.Partition, error) {
	file, err := utils.GetFile(pdm.getPartitionsFileName(db, blob))
	if err != nil {
		return models.Partition{}, err
	}

	var partition models.Partition
	err = json.Unmarshal(file, &partition)
	return partition, err
}

func (pdm *partitionDiskManager) GetByHashKey(db string, blob string, hashKeyFileName string) (models.PartitionPages, error) {
	file, err := utils.GetFile(fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName))
	if err != nil {
		return nil, err
	}

	var partitionPages models.PartitionPages
	err = json.Unmarshal(file, &partitionPages)
	return partitionPages, err
}

func (pdm *partitionDiskManager) GetAll(db string, blob string) ([]string, error) {
	return utils.GetDirectoryContents(pdm.getPartitionsDirectoryName(db, blob))
}

func (pdm *partitionDiskManager) createHashKey(db string, blob string, hashKeyFileName string) (models.PartitionPages, error) {
	hashKeyFilePath := fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName)
	err := utils.CreateFile(hashKeyFilePath)
	if err != nil {
		return nil, err
	}

	partitionPages := models.PartitionPages{}
	partitionPagesData, err := json.Marshal(partitionPages)
	if err != nil {
		return nil, err
	}

	return partitionPages, utils.WriteFile(hashKeyFilePath, partitionPagesData)
}

func (pdm *partitionDiskManager) Remove(db string, blob string, hashKeyFileName string, pageFileName string) error {
	partitionPages, err := pdm.GetByHashKey(db, blob, hashKeyFileName)
	if err != nil {
		return err
	}

	for i := 0; i < len(partitionPages); i++ {
		if partitionPages[i].FileName == pageFileName {
			copy(partitionPages[i:], partitionPages[i+1:])
			partitionPages[len(partitionPages)-1] = models.PartitionPageItem{}
			partitionPages = partitionPages[:len(partitionPages)-1]
			partitionPagesData, _ := json.Marshal(partitionPages)
			return utils.WriteFile(fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName), partitionPagesData)
		}
	}
	return nil
}

func (pdm *partitionDiskManager) Delete(db string, blob string, hashKeyFileName string) error {
	return utils.DeleteFile(fmt.Sprintf("%s/%s", pdm.getPartitionsDirectoryName(db, blob), hashKeyFileName))
}

func (pdm *partitionDiskManager) GetHashKey(partition models.Partition, pageRecord models.PageRecord) (string, error) {
	hashKey := ""
	for _, key := range partition.Keys {
		pageRecordItem, ok := pageRecord[key]
		if !ok {
			return "", fmt.Errorf("%s not found in page record", key)
		}
		hash := sha1.New()
		hash.Write([]byte(fmt.Sprintf("%+v", pageRecordItem)))
		hashKey += base64.URLEncoding.EncodeToString(hash.Sum(nil))
	}
	return fmt.Sprintf("%s.json", hashKey), nil
}

func (pdm *partitionDiskManager) getPartitionsFileName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, partitionsFile)
}

func (pdm *partitionDiskManager) getPartitionsDirectoryName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, partitionsDirectory)
}
