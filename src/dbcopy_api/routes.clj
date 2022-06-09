(ns dbcopy-api.routes
  (:require [compojure.core :refer [defroutes GET POST]]
            [ring.util.response :refer [response not-found]]
            [dbcopy-api.utils :as u]
            [dbcopy-api.dependencies :as deps]
            [dbcopy-api.ingest :as ing]
            [dbcopy-api.map-db :as mdb]
            [dbcopy-api.egest :as eg]))

(def GET-root-healthz
  (GET "/" []
    (response "I'm here")))

(def GET-db
  (GET "/db" [:as {{db :db} :in-mem-db}]
    (response {:db @db})))

(def POST-db
  (POST "/db" [:as {{db :db} :in-mem-db body :body}]
    (reset! db body)
    (response {:db @db})))

(def GET-cols
  (GET "/cols" [:as {{db :db} :in-mem-db}]
    (response (deps/get-all-cols @db))))

(def GET-referenced-cols
  (GET "/referenced-cols" [:as {{db :db} :in-mem-db}]
    (response (deps/get-referenced-cols @db))))

(def POST-deps
  (POST "/deps" [:as {{deps :deps} :in-mem-db body :body}]
    (reset! deps body)
    (response {:deps @deps})))

(def POST-ingest
  (POST "/ingest" [:as {{:keys [db deps slurped-data]} :in-mem-db body :body}]
    (let [{:keys [table ids]} {:table ["public" "school" "id"] :ids [1]}
          table (u/vec-kw table)
          table-short (u/vec-kw (take 2 table))]
      (reset! slurped-data (ing/slurp-data @db
                                           @deps
                                           (mdb/make-dag @deps table-short)
                                           (mdb/make-primary-keys @deps)
                                           {table ids}))
      (response {:slurped-data @slurped-data}))))

(def POST-egest
  (POST "/egest" [:as {{:keys [db deps slurped-data spat-rows]} :in-mem-db body :body}]
    (let [{:keys [table seed-values]} {["public" "school"]
                                       [{:id 2
                                         :name "Ridge View Elementary"}]}
          table (u/vec-kw table)]
      (reset! spat-rows (eg/insert-rows @db
                                        @deps
                                        (mdb/make-dag @deps table)
                                        (mdb/make-primary-keys @deps)
                                        @slurped-data
                                        {table seed-values}))
      (response {:spat-data @spat-rows}))))

(def GET-404
  (GET "*" []
    (not-found nil)))

(def POST-404
  (POST "*" []
    (not-found nil)))

(defroutes routes
  #'GET-root-healthz
  #'GET-db
  #'POST-db
  #'GET-cols
  #'GET-referenced-cols
  #'POST-deps
  #'POST-ingest
  #'POST-egest
  #'GET-404
  #'POST-404)
