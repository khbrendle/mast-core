package mast

import "time"

type Table struct {
	TableID    string    `json:"table_id,omitempty"`
	DatabaseID string    `json:"database_id,omitempty"`
	Name       string    `json:"table_name,omitempty"`
	SchemaName string    `json:"schema_name,omitempty"`
	CreatedAt  time.Time `json:"created_at,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
	DeletedAt  time.Time `json:"deleted_at,omitempty"`
}
