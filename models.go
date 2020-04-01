package mast

import (
	"encoding/json"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/lib/pq"
	"github.com/rs/xid"
)

type Database struct {
	DatabaseID   string    `json:"database_id,omitempty" gorm:"column:database_id"`
	DatabaseName string    `json:"database_name,omitempty" gorm:"column:database_name"`
	Note         *string   `json:"note,omitempty" gorm:"column:note"`
	CreatedAt    time.Time `json:"created_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
	DeletedAt    time.Time `json:"deleted_at,omitempty"`
}

func (i *Database) SetDatabaseID(id string) {
	i.DatabaseID = id
}

type Table struct {
	TableID      string    `json:"table_id,omitempty" gorm:"column:table_id"`
	DatabaseID   string    `json:"database_id,omitempty" gorm:"column:database_id"`
	TableName    string    `json:"table_name,omitempty" gorm:"column:table_name"`
	SchemaName   string    `json:"schema_name,omitempty" gorm:"column:schema_name"`
	Note         string    `json:"note,omitempty" gorm:"column:note"`
	DatabaseName string    `json:"-"`
	CreatedAt    time.Time `json:"created_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
	DeletedAt    time.Time `json:"deleted_at,omitempty"`
}

func (i *Table) SetDatabaseID(id string) {
	i.DatabaseID = id
}
func (i *Table) SetTableID(id string) {
	i.TableID = id
}

type Field struct {
	TableID      string    `json:"table_id,omitempty" gorm:"column:table_id"`
	FieldID      string    `json:"field_id,omitempty" gorm:"column:field_id"`
	FieldName    string    `json:"field_name,omitempty" gorm:"column:field_name"`
	DataType     string    `json:"data_type,omitempty" gorm:"column:data_type"`
	Note         string    `json:"note,omitempty" gorm:"column:note"`
	DatabaseName string    `json:"-"`
	SchemaName   string    `json:"-"`
	TableName    string    `json:"-"`
	CreatedAt    time.Time `json:"created_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
	DeletedAt    time.Time `json:"deleted_at,omitempty"`
}

type Relation struct {
	FromID   string         `json:"from,omitempty" gorm:"column:from"`
	ToID     string         `json:"to,omitempty" gorm:"column:to"`
	Relation pq.StringArray `json:"relation,omitempty" gorm:"column:relation;type:string[]"`
	// RelationVal []string `json:"relation,omitempty" gorm:"-"`
	Description string `json:"description,omitempty" gorm:"column:description"`
	Attr        []byte `json:"attr,omitempty" gorm:"column:attr"`
}

type GraphNode struct {
	Type  string `json:"type,omitempty" gorm:"column:type"`
	ID    string `json:"id,omitempty" gorm:"column:id"`
	Label string `json:"label,omitempty" gorm:"column:label"`
	Color string `json:"color,omitempty" gorm:"column:color"`
}

// func (i *Field) SetDatabaseID(id string) {
// 	i.DatabaseID = id
// }
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
