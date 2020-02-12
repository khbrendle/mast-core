package mast

import (
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	pg "github.com/lib/pq"
)

func (a *API) DBConnect() error {
	fmt.Println("connecting to database")
	var err error
	if a.DB, err = gorm.Open("postgres", "host=localhost port=5432 user=postgres dbname=mast password=webapp sslmode=disable"); err != nil {
		return err
	}
	return nil
}

func (a *API) CreateDatabase(dbs []Database) error {
	fmt.Printf("Creating records for %+v\n", dbs)
	var err error
	for _, r := range dbs {
		tx := a.DB.Begin()
		if err = tx.Table(`entities.database`).Create(&r).Error; err != nil {
			tx.Rollback()
			switch err.(type) {
			case *pg.Error:
				return err.(*pg.Error)
			default:
				return err
			}
		}
		fmt.Println("comitting")
		tx.Commit()
	}
	return nil
}

func (a *API) CreateTable(tables []Table) error {
	fmt.Printf("Creating records for %+v\n", tables)
	var err error
	for _, r := range tables {
		tx := a.DB.Begin()
		if err = tx.Table(`entities.table`).Create(&r).Error; err != nil {
			tx.Rollback()
			switch err.(type) {
			case *pg.Error:
				return err.(*pg.Error)
			default:
				return err
			}
		}
		fmt.Println("comitting")
		tx.Commit()
	}
	return nil
}

func (a *API) CreateField(fields []*Field) error {
	fmt.Printf("Creating records for %+v\n", fields)
	var err error
	for _, r := range fields {
		tx := a.DB.Begin()
		if err = tx.Table(`entities.field`).Create(&r).Error; err != nil {
			tx.Rollback()
			switch err.(type) {
			case *pg.Error:
				return err.(*pg.Error)
			default:
				return err
			}
		}
		fmt.Println("comitting")
		tx.Commit()
	}
	return nil
}

// func (a *API) GetTransform(fieldID string) (FieldTransform, error) {
// 	var err error
// 	var res FieldTransform
// 	err = a.DB.Table(`entities.field_transform`).Where(`field_id = ? and deleted_at is null`).Scan(&res).Error
// 	if err != nil {
// 		return FieldTransform{}, err
// 	}
// 	return res, nil
// }

// this will always create new transformations
// if it currently exists then delete old record and create new
func (a *API) CreateFieldTransform(ft FieldTransform) error {
	var err error

	// start transaction
	tx := a.DB.Begin()
	// if new object then CreatedAt would be null
	if (ft.Model.CreatedAt != time.Time{}) {
		// so if not new then delete old record
		if err = tx.Table(`entities.field_transform`).Delete(&ft).Error; err != nil {
			tx.Rollback()
			return err
		}
		// reset gorm.Model info
		ft.Model = gorm.Model{}
	}

	// create new record
	err = tx.Table(`entities.field_transform`).Create(&ft).Error
	if err != nil {
		tx.Rollback()
		return err
	}
	// finally commit
	tx.Commit()
	return nil
}
