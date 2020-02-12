package mast

func (api *API) AddRoutes() {
	api.SubRouter.HandleFunc("/health", api.GetHealth).Methods("Get")

	api.SubRouter.HandleFunc("/upload", api.PostUpload).Methods("Post")

	api.SubRouter.HandleFunc("/database", api.GetDatabase).Methods("Get")
	api.SubRouter.HandleFunc("/table", api.GetTable).Methods("Get")
	api.SubRouter.HandleFunc("/field", api.GetField).Methods("Get")
	api.SubRouter.HandleFunc("/field/transform", api.GetFieldTransform).Methods("Get")
	api.SubRouter.HandleFunc("/field/transform", api.PostFieldTransform).Methods("POST")
}
