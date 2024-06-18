package managers

import (
	"fmt"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/utils"
	"sync"
)

type DBDiskManager interface {
	Create(db string) error
	Delete(db string) error
	GetAll() ([]string, error)
}

type dbDiskManager struct {
	dataLocation string
}

var dbDiskManagerInstance *dbDiskManager

func CreateDBDiskManager(dataLocation string) DBDiskManager {
	sync.OnceFunc(func() {
		dbDiskManagerInstance = &dbDiskManager{dataLocation: dataLocation}
	})()
	return dbDiskManagerInstance
}

func (ddm *dbDiskManager) Create(db string) error {
	return utils.CreateDir(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
}

func (ddm *dbDiskManager) Delete(db string) error {
	return utils.DeleteDirectory(fmt.Sprintf("%s/%s", ddm.dataLocation, db))
}

func (ddm *dbDiskManager) GetAll() ([]string, error) {
	return utils.GetDirectoryContents(ddm.dataLocation)
}
