package mast

import "time"

type Database struct {
	DatabaseID string    `json:"database_id,omitempty"`
	Name       string    `json:"database_name,omitempty"`
	Note       string    `json:"note,omitempty"`
	CreatedAt  time.Time `json:"created_at,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
	DeletedAt  time.Time `json:"deleted_at,omitempty"`
}
