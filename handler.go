package mast

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/rs/xid"
)

// GetHealth just returns 200 if is accessible
func (api *API) GetHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// PostUpload ...
func (api *API) PostUpload(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(32 << 20) // limit your max input length!
	fmt.Printf("customField: %s\n", r.PostForm["CustomField"])

	var buf bytes.Buffer
	// in your case file would be fileupload
	file, header, err := r.FormFile("file")
	if err != nil {
		HandleAPIError(w, http.StatusBadRequest, "error reading file from request", err)
		return
	}
	defer file.Close()
	// fmt.Printf("header: %+v\n", header)
	name := strings.Split(header.Filename, ".") // extension would be index 1
	fmt.Printf("File name %s\n", name[0])

	io.Copy(&buf, file)

	c := csv.NewReader(&buf)

	var dbMap = make(map[string]*Database)
	var dbs []Database
	var tableMap = make(map[string]*Table)
	var tables []Table
	var fields []*Field

	for itr := 0; ; itr++ {
		// fmt.Printf("itr: %v - ", itr)
		// testing 5 rows
		r, err := c.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			HandleAPIError(w, http.StatusBadRequest, "error reading records", err)
			return
		}
		// if itr > 5 {
		// 	break
		// }
		if itr > 0 {
			// read recrod
			dbMap[r[0]] = &Database{
				DatabaseName: r[0],
			}
			tableMap[fmt.Sprintf(`%s.%s`, r[1], r[2])] = &Table{
				DatabaseName: r[0],
				SchemaName:   r[1],
				TableName:    r[2],
			}
			// fmt.Printf("db:%s;schema:%s;table:%s;field:%s;", r[0], r[1], r[2], r[3])
			fields = append(fields, &Field{
				DatabaseName: r[0],
				SchemaName:   r[1],
				TableName:    r[2],
				FieldID:      xid.New().String(),
				FieldName:    r[3],
			})
		} else {
			// read header
		}
		// fmt.Printf("\n")
	}
	var id string
	// create DB records
	for k, v := range dbMap {
		id = xid.New().String()
		dbMap[k].SetDatabaseID(id)
		dbs = append(dbs, *v)
	}
	// create Table records
	for _, v := range tableMap {
		v.SetDatabaseID(dbMap[v.DatabaseName].DatabaseID)
		id = xid.New().String()
		v.SetTableID(id)
		tables = append(tables, *v)
	}
	// add DB & Table IDs to fields
	for _, v := range fields {
		v.SetDatabaseID(dbMap[v.DatabaseName].DatabaseID)
		v.SetTableID(tableMap[fmt.Sprintf(`%s.%s`, v.SchemaName, v.TableName)].TableID)
	}

	fmt.Println("\nfinished reading csv")
	buf.Reset()

	if err = api.CreateDatabase(dbs); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error creating database records", err)
		return
	}

	if err = api.CreateTable(tables); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error creating database records", err)
		return
	}

	if err = api.CreateField(fields); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error creating database records", err)
		return
	}

	// var b []byte
	// if b, err = json.Marshal(dbs); err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("dbs: %s\n", string(b))
	// if b, err = json.Marshal(tables); err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("tables: %s\n", string(b))
	// if b, err = json.Marshal(fields); err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("fields: %s\n", string(b))

	w.WriteHeader(http.StatusOK)
}

func (a *API) GetDatabase(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	databaseID := r.FormValue("database_id")

	var o []Database
	var err error
	if err = a.DB.Table(`entities.database`).Where("database_id = ?", databaseID).Find(&o).Error; err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error getting data", err)
		return
	}

	var b []byte
	if b, err = json.Marshal(&o); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error creating response", err)
		return
	}

	w.Header().Set("Content-Type", "text/json; charset=utf-8")
	w.Write(b)
}

func (a *API) GetTable(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	keys := []string{"database_id", "table_id"}

	var o []Table
	var err error
	var v string
	tx := a.DB.Table(`entities.table`)
	for _, k := range keys {
		v = r.FormValue(k)
		// fmt.Printf("K: %s; V: %s\n", k, v)
		// if unknown v will be empty string
		if v != "" {
			tx = tx.Where(fmt.Sprintf(`%s = ?`, k), v)
		}
	}
	tx = tx.Where("deleted_at is null")
	if err = tx.Scan(&o).Error; err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error getting data", err)
		return
	}

	var b []byte
	if b, err = json.Marshal(&o); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error creating response", err)
		return
	}

	w.Header().Set("Content-Type", "text/json; charset=utf-8")
	w.Write(b)
}

func (a *API) GetField(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	keys := []string{"database_id", "table_id", "field_id"}

	var o []Field
	var err error
	var v string
	tx := a.DB.Table(`entities.field`)
	for _, k := range keys {
		v = r.FormValue(k)
		// fmt.Printf("K: %s; V: %s\n", k, v)
		// if unknown v will be empty string
		if v != "" {
			tx = tx.Where(fmt.Sprintf(`%s = ?`, k), v)
		}
	}
	tx = tx.Where("deleted_at is null")
	if err = tx.Scan(&o).Error; err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error getting data", err)
		return
	}

	var b []byte
	if b, err = json.Marshal(&o); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error creating response", err)
		return
	}

	w.Header().Set("Content-Type", "text/json; charset=utf-8")
	w.Write(b)
}

func (a *API) GetFieldTransform(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	// TODO: think about changing normalization level
	// would like to have this single query get mapping by db, table, and/or field
	// keys := []string{"database_id", "table_id", "field_id"}
	keys := []string{"field_id"}

	var o []FieldTransform
	var err error
	var v string
	tx := a.DB.Table(`entities.field_transform`)
	for _, k := range keys {
		v = r.FormValue(k)
		// if unknown v will be empty string
		if v != "" {
			tx = tx.Where(fmt.Sprintf(`%s = ?`, k), v)
		}
	}
	tx = tx.Where("deleted_at is null")
	if err = tx.Scan(&o).Error; err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error getting data", err)
		return
	}

	var b []byte
	if b, err = json.Marshal(&o); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error creating response", err)
		return
	}

	w.Header().Set("Content-Type", "text/json; charset=utf-8")
	w.Write(b)
}

func (a *API) PostFieldTransform(w http.ResponseWriter, r *http.Request) {
	var ft FieldTransform
	var err error

	fmt.Printf("post body: %v\n", r.Body)

	if err = json.NewDecoder(r.Body).Decode(&ft); err != nil {
		HandleAPIError(w, http.StatusBadRequest, "error decoding request body", err)
		return
	}

	// add transform_id
	ft.CreateTransformID()

	if err = a.CreateFieldTransform(ft); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error writing transform to database", err)
		return
	}

	w.WriteHeader(http.StatusOK)
}
