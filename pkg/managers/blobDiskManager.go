package managers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/utils"
	"sync"
)

type BlobDiskManager interface {
	Create(db string, blob string) error
	Delete(db string, blob string) error
	GetByDB(db string) ([]string, error)
}

type blobDiskManager struct {
	dataLocation string
}

var blobDiskManagerInstance *blobDiskManager

func CreateBlobDiskManager(dataLocation string) BlobDiskManager {
	sync.OnceFunc(func() {
		blobDiskManagerInstance = &blobDiskManager{dataLocation: dataLocation}
	})
	return blobDiskManagerInstance
}

func (bdm *blobDiskManager) Create(db string, blob string) error {
	return utils.CreateDir(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob))
}

func (bdm *blobDiskManager) Delete(db string, blob string) error {
	return utils.DeleteDirectory(fmt.Sprintf("%s/%s/%s", bdm.dataLocation, db, blob))
}

func (bdm *blobDiskManager) GetByDB(db string) ([]string, error) {
	return utils.GetDirectoryContents(fmt.Sprintf("%s/%s", bdm.dataLocation, db))
}
