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

// PostUpload will accept a csv file and write the information in to the database
// expecting : database,schema,table,field
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
		// fmt.Printf(`'%s%s%s%s%s';`, r[0], r[1], r[2], r[3], r[4])
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
				FieldID:    xid.New().String(),
				SchemaName: r[1],
				TableName:  r[2],
				FieldName:  r[3],
				DataType:   r[4],
			})
		} else {
			// read header
		}
		// fmt.Printf("\n")
	}
	var id string
	// create DB records
	var i int
	for k, v := range dbMap {
		fmt.Printf("db %d", i)
		id = xid.New().String()
		dbMap[k].SetDatabaseID(id)
		dbs = append(dbs, *v)
		i++
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
		// v.SetDatabaseID(dbMap[v.DatabaseName].DatabaseID)
		// fmt.Printf("%s.%s\n", v.SchemaName, v.TableName)
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
	keys := []string{"database_id"}

	var o []Database
	var err error
	var v string
	tx := a.DB.Table(`entities.database`)
	for _, k := range keys {
		v = r.FormValue(k)
		if v != "" {
			tx = tx.Where(fmt.Sprintf(`%s = ?`, k), v)
		}
	}
	tx = tx.Order("database_name")
	if err = tx.Find(&o).Error; err != nil {
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
	tx = tx.Where("deleted_at is null").Order("table_name")
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
	tx = tx.Where("deleted_at is null").Order("field_name")
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

func (a *API) PostDatabase(w http.ResponseWriter, r *http.Request) {
	var d Database
	var err error

	fmt.Printf("post database body: %v\n", r.Body)

	if err = json.NewDecoder(r.Body).Decode(&d); err != nil {
		HandleAPIError(w, http.StatusBadRequest, "error decoding request body", err)
		return
	}
	fmt.Printf("post body decoded: %v\n", d)

	if err = a.CreateDatabase([]Database{d}); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error writing transform to database", err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *API) PostTable(w http.ResponseWriter, r *http.Request) {
	var t Table
	var err error

	fmt.Printf("post body: %v\n", r.Body)

	if err = json.NewDecoder(r.Body).Decode(&t); err != nil {
		HandleAPIError(w, http.StatusBadRequest, "error decoding request body", err)
		return
	}

	if err = a.CreateTable([]Table{t}); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error writing transform to database", err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *API) PostField(w http.ResponseWriter, r *http.Request) {
	var f Field
	var err error

	fmt.Printf("post body: %v\n", r.Body)

	if err = json.NewDecoder(r.Body).Decode(&f); err != nil {
		HandleAPIError(w, http.StatusBadRequest, "error decoding request body", err)
		return
	}

	if err = a.CreateField([]*Field{&f}); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error writing transform to database", err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *API) GetRelation(w http.ResponseWriter, r *http.Request) {
	var rels []Relation
	var err error

	if err = a.DB.Table(`entities.relation`).Select(`from_id "from", to_id "to", relation`).Scan(&rels).Error; err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error getting relations from database", err)
	}

	var b []byte
	if b, err = json.Marshal(&rels); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error creating response", err)
		return
	}

	w.Header().Set("Content-Type", "text/json; charset=utf-8")
	w.Write(b)
}

func (a *API) GetNode(w http.ResponseWriter, r *http.Request) {
	var nodes []GraphNode
	var err error

	if err = a.DB.Table(`entities.all`).Scan(&nodes).Error; err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error getting relations from database", err)
	}

	var b []byte
	if b, err = json.Marshal(&nodes); err != nil {
		HandleAPIError(w, http.StatusInternalServerError, "error creating response", err)
		return
	}

	w.Header().Set("Content-Type", "text/json; charset=utf-8")
	w.Write(b)
}

func (a *API) EntitySearch(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	keys := []string{"label"}

	var o []GraphNode
	var err error
	var v string
	tx := a.DB.Table(`entities.all`)
	for _, k := range keys {
		v = r.FormValue(k)
		// fmt.Printf("K: %s; V: %s\n", k, v)
		if (v != "") && (k == "label") {
			tx = tx.Where(fmt.Sprintf(`label like '%%%s%%'`, v))
		}
	}
	// tx = tx.Order("field_name")
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
