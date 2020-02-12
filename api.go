package mast

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jinzhu/gorm"
)

type API struct {
	Version   string
	Port      string
	DB        *gorm.DB
	Router    *mux.Router
	SubRouter *mux.Router
	// CORS details
	AllowedHeaders []string
	AllowedMethods []string
	AllowedOrigins []string
}

func (api *API) Init() error {
	api.Version = "0"
	api.Port = "3000"
	api.Router = mux.NewRouter()
	// CORS options
	api.AllowedHeaders = []string{"X-Requested-With", "Content-Type", "Authorization"}
	api.AllowedMethods = []string{"GET", "POST", "PUT", "HEAD", "OPTIONS"}
	api.AllowedOrigins = []string{"*"}

	api.SubRouter = api.Router.PathPrefix(fmt.Sprintf("/v%s/", api.Version)).Subrouter()

	api.AddRoutes()

	var err error
	if err = api.DBConnect(); err != nil {
		return err
	}

	return nil
}

// Run ...
func (api *API) Run() {
	fmt.Printf("serving API at port %s\n", api.Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", api.Port), handlers.CORS(handlers.AllowedHeaders(api.AllowedHeaders), handlers.AllowedMethods(api.AllowedMethods), handlers.AllowedOrigins(api.AllowedOrigins))(api.Router)))
}
