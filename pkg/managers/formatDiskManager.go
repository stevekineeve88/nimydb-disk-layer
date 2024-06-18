package managers

import (
	"encoding/json"
	"fmt"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/models"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/utils"
	"sync"
)

const (
	formatFile = "format.json"
)

type FormatDiskManager interface {
	Create(db string, blob string, format models.Format) error
	Get(db string, blob string) (models.Format, error)
}

type formatDiskManager struct {
	dataLocation string
}

var formatDiskManagerInstance *formatDiskManager

func CreateFormatDiskManager(dataLocation string) FormatDiskManager {
	sync.OnceFunc(func() {
		formatDiskManagerInstance = &formatDiskManager{dataLocation: dataLocation}
	})()
	return formatDiskManagerInstance
}

func (fdm *formatDiskManager) Create(db string, blob string, format models.Format) error {
	formatData, err := json.Marshal(format)
	if err != nil {
		return err
	}
	filePath := fmt.Sprintf("%s/%s/%s/%s", fdm.dataLocation, db, blob, formatFile)
	err = utils.CreateFile(filePath)
	if err != nil {
		return err
	}
	return utils.WriteFile(filePath, formatData)
}

func (fdm *formatDiskManager) Get(db string, blob string) (models.Format, error) {
	var format models.Format
	file, err := utils.GetFile(fmt.Sprintf("%s/%s/%s/%s", fdm.dataLocation, db, blob, formatFile))
	if err != nil {
		return format, err
	}

	err = json.Unmarshal(file, &format)
	return format, err
}
