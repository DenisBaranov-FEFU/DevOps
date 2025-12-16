package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"news_app/pkg/storage"

	"github.com/gorilla/mux"
)

type API struct {
	storage storage.Interface
	router  *mux.Router
}

func New(storage storage.Interface) *API {
	api := &API{
		storage: storage,
		router:  mux.NewRouter(),
	}
	api.endpoints()
	return api
}

func (api *API) endpoints() {
	api.router.HandleFunc("/posts", api.postsHandler).Methods("GET", "POST")
	api.router.HandleFunc("/posts/{id}", api.postHandler).Methods("GET", "PUT", "DELETE")
	api.router.HandleFunc("/version", api.versionHandler).Methods("GET")
}

func (api *API) Router() *mux.Router {
	return api.router
}

func (api *API) postsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		api.getPosts(w, r)
	case "POST":
		api.addPost(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *API) postHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Invalid post ID", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case "GET":
		api.getPost(w, r, id)
	case "PUT":
		api.updatePost(w, r, id)
	case "DELETE":
		api.deletePost(w, r, id)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *API) getPosts(w http.ResponseWriter, r *http.Request) {
	posts, err := api.storage.Posts(r.Context())
	if err != nil {
		log.Printf("Get posts error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, http.StatusOK, posts)
}

func (api *API) getPost(w http.ResponseWriter, r *http.Request, id int) {
	post, err := api.storage.GetPost(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	respondJSON(w, http.StatusOK, post)
}

func (api *API) addPost(w http.ResponseWriter, r *http.Request) {
	var post storage.Post
	if err := json.NewDecoder(r.Body).Decode(&post); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id, err := api.storage.AddPost(r.Context(), post)
	if err != nil {
		log.Printf("Add post error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	post.ID = id
	respondJSON(w, http.StatusCreated, post)
}

func (api *API) updatePost(w http.ResponseWriter, r *http.Request, id int) {
	var post storage.Post
	if err := json.NewDecoder(r.Body).Decode(&post); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	post.ID = id

	if err := api.storage.UpdatePost(r.Context(), post); err != nil {
		log.Printf("Update post error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, http.StatusOK, post)
}

func (api *API) deletePost(w http.ResponseWriter, r *http.Request, id int) {
	if err := api.storage.DeletePost(r.Context(), id); err != nil {
		log.Printf("Delete post error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (api *API) versionHandler(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{
		"version": "1.0.0",
		"service": "News App API",
	})
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("JSON encode error: %v", err)
	}
}