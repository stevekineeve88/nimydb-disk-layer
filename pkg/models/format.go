package models

type Format map[string]FormatItem

type FormatItem struct {
	KeyType string `json:"keyType"`
}
