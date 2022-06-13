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

(def GET-primary-key-cols
  (GET "/primary-key-cols" [table-col :as {{db :db} :in-mem-db}]
    (response (deps/get-primary-key-cols @db table-col))))

(def POST-root-table-row
  (POST "/root-table-row" [:as {{root-table-row :root-table-row} :in-mem-db body :body}]
    (reset! root-table-row body)
    (response {:root-table-row @root-table-row})))

(def GET-raw-deps
  (GET "/raw-deps" [:as {{db :db} :in-mem-db}]
    (response (deps/deps->json (deps/build-deps (deps/get-referenced-cols @db))))))

(def POST-deps
  (POST "/deps" [:as {{deps :deps} :in-mem-db body :body}]
    (reset! deps (deps/json->deps body))
    (response {:deps @deps})))

(def POST-ingest
  (POST "/ingest" [:as {{:keys [db root-table-row deps slurped-data]} :in-mem-db}]
    (let [{:keys [table ids]} @root-table-row
          table (u/vec-kw table)
          table-short (u/vec-kw (take 2 table))]
      (reset! slurped-data (ing/slurp-data @db
                                           @deps
                                           (mdb/make-dag @deps table-short)
                                           (mdb/make-primary-keys @deps)
                                           {table ids}))
      (response {:rows (reduce + (map count (vals (:data @slurped-data))))}))))

(def POST-ingest-test
  (POST "/ingest-test" [:as {{:keys [db root-table-row deps slurped-data]} :in-mem-db}]
    (let [{:keys [table ids]} @root-table-row
          table (u/vec-kw table)
          table-short (u/vec-kw (take 2 table))
          {result :data} (ing/slurp-data @db
                                         @deps
                                         (mdb/make-dag @deps table-short)
                                         (mdb/make-primary-keys @deps)
                                         {table ids}
                                         false)]
      (response (into {} (map (fn [[k v]] [(u/make-table-kw k) v]) result))))))

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
  #'GET-primary-key-cols
  #'POST-root-table-row
  #'GET-raw-deps
  #'POST-deps
  #'POST-ingest
  #'POST-ingest-test
  #'POST-egest
  #'GET-404
  #'POST-404)
