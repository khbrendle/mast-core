package mast

import (
	"encoding/json"

	"github.com/jinzhu/gorm"
	"github.com/rs/xid"
)

type Database struct {
	gorm.Model
	DatabaseID   string  `json:"database_id" gorm:"column:database_id"`
	DatabaseName string  `json:"database_name" gorm:"column:database_name"`
	Note         *string `json:"note" gorm:"column:note"`
}

func (i *Database) SetDatabaseID(id string) {
	i.DatabaseID = id
}

type Table struct {
	gorm.Model
	DatabaseID   string `json:"database_id" gorm:"column:database_id"`
	DatabaseName string `json:"database_name" gorm:"column:database_name"`
	SchemaName   string `json:"schema_name" gorm:"column:schema_name"`
	TableID      string `json:"table_id" gorm:"column:table_id"`
	TableName    string `json:"table_name" gorm:"column:table_name"`
	Note         string `json:"note" gorm:"column:note"`
}

func (i *Table) SetDatabaseID(id string) {
	i.DatabaseID = id
}
func (i *Table) SetTableID(id string) {
	i.TableID = id
}

type Field struct {
	gorm.Model
	DatabaseID   string `json:"database_id" gorm:"column:database_id"`
	DatabaseName string `json:"database_name" gorm:"column:database_name"`
	SchemaName   string `json:"schema_name" gorm:"column:schema_name"`
	TableID      string `json:"table_id" gorm:"column:table_id"`
	TableName    string `json:"table_name" gorm:"column:table_name"`
	FieldID      string `json:"field_id" gorm:"column:field_id"`
	FieldName    string `json:"field_name" gorm:"column:field_name"`
	Note         string `json:"note" gorm:"column:note"`
}

func (i *Field) SetDatabaseID(id string) {
	i.DatabaseID = id
}
func (i *Field) SetTableID(id string) {
	i.TableID = id
}
func (i *Field) SetFieldID(id string) {
	i.FieldID = id
}

type FieldTransform struct {
	gorm.Model
	FieldID     string          `json:"field_id" gorm:"column:field_id"`
	TransformID string          `json:"transform_id" gorm:"column:transform_id"`
	Transform   json.RawMessage `json:"transform" gorm:"column:transform"`
}

func (i *FieldTransform) CreateTransformID() {
	i.TransformID = xid.New().String()
}

type TableTransform struct {
	gorm.Model
	TableID string `json:"table_id" gorm:"column:table_id"`
}
