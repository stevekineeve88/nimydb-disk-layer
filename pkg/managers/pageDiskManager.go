package managers

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/models"
	"github.com/stevekineeve88/nimydb-disk-layer.git/pkg/utils"
	"sync"
)

const (
	pagesFile      = "pages.json"
	pagesDirectory = "pages"
)

type PageDiskManager interface {
	Initialize(db string, blob string) error
	Create(db string, blob string) (string, error)
	GetAll(db string, blob string) (models.Pages, error)
	GetData(db string, blob string, pageFileName string) (models.PageRecords, error)
	WriteData(db string, blob string, pageFileName string, data models.PageRecords) error
	Delete(db string, blob string, pageFileName string) error
}

type pageDiskManager struct {
	dataLocation string
}

var pageDiskManagerInstance *pageDiskManager

func CreatePageDiskManager(dataLocation string) PageDiskManager {
	sync.OnceFunc(func() {
		pageDiskManagerInstance = &pageDiskManager{dataLocation: dataLocation}
	})
	return pageDiskManagerInstance
}

func (pdm *pageDiskManager) Initialize(db string, blob string) error {
	pagesFilePath := pdm.getPagesFileName(db, blob)
	if err := utils.CreateFile(pagesFilePath); err != nil {
		return err
	}

	var pages models.Pages
	pagesData, err := json.Marshal(pages)
	if err != nil {
		return err
	}
	if err := utils.WriteFile(pagesFilePath, pagesData); err != nil {
		return nil
	}

	return utils.CreateDir(pdm.getPagesDirectoryName(db, blob))
}

func (pdm *pageDiskManager) Create(db string, blob string) (string, error) {
	newPageFile := fmt.Sprintf("%s.json", uuid.New().String())
	newPageFilePath := fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), newPageFile)
	if err := utils.CreateFile(newPageFilePath); err != nil {
		return "", err
	}
	var pageRecords models.PageRecords
	pageRecordsData, _ := json.Marshal(pageRecords)
	if err := utils.WriteFile(newPageFilePath, pageRecordsData); err != nil {
		return "", err
	}

	pages, err := pdm.GetAll(db, blob)
	if err != nil {
		return "", err
	}
	pages = append(pages, models.PageItem{FileName: newPageFile})
	pagesData, _ := json.Marshal(pages)
	err = utils.WriteFile(pdm.getPagesFileName(db, blob), pagesData)
	return newPageFile, err
}

func (pdm *pageDiskManager) GetAll(db string, blob string) (models.Pages, error) {
	var pages models.Pages
	pagesFilePath := pdm.getPagesFileName(db, blob)
	file, err := utils.GetFile(pagesFilePath)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(file, &pages)
	return pages, err
}

func (pdm *pageDiskManager) GetData(db string, blob string, pageFileName string) (models.PageRecords, error) {
	var pageRecords models.PageRecords
	file, err := utils.GetFile(fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), pageFileName))
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(file, &pageRecords)
	return pageRecords, err
}

func (pdm *pageDiskManager) WriteData(db string, blob string, pageFileName string, data models.PageRecords) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return utils.WriteFile(fmt.Sprintf("%s/%s", pdm.getPagesDirectoryName(db, blob), pageFileName), dataBytes)
}

func (pdm *pageDiskManager) Delete(db string, blob string, pageFileName string) error {
	//TODO: FINISH THIS!!!
	return nil
}

func (pdm *pageDiskManager) getPagesFileName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, pagesFile)
}

func (pdm *pageDiskManager) getPagesDirectoryName(db string, blob string) string {
	return fmt.Sprintf("%s/%s/%s/%s", pdm.dataLocation, db, blob, pagesDirectory)
}
