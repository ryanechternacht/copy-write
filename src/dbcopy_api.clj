(ns dbcopy-api
  (:require [cli-matic.core :as cli]
            [clojure.string :as str]
            [clj-yaml.core :as yaml]
            [dbcopy-api.db :as db]
            [dbcopy-api.dependencies :as dep]
            [dbcopy-api.ingest :as ing]
            [dbcopy-api.map-db :as mdb]
            [dbcopy-api.utils :as u]
            [honey.sql.helpers :as h]))

;; clj -M -m dbcopy-api test-db --db=db.yaml

(defn test-db [{:keys [db]}]
  (try
    (let [{db-val :db} (-> db slurp yaml/parse-string)
          #_{:clj-kondo/ignore [:unused-binding]}
          trial-select (-> (h/select :*)
                           (h/from :information_schema.columns)
                           (h/limit 1)
                           (db/->execute db-val))]
      (println "Everything looks good")
      0)
    (catch Exception e
      (println "Found an issue:" (.getMessage e))
      -1)))

(defn list-tables [{db-file :db tables-file :file}]
  (try
    (let [{db :db} (-> db-file slurp yaml/parse-string)
          tables (map :table (dep/get-all-tables db))]
      (->> {:root-table nil
            :root-ids [nil]
            :tables tables}
           (#(yaml/generate-string % :dumper-options {:flow-style :block}))
           (spit tables-file))
      (println "Generated table list at" tables-file)
      0)
    (catch Exception e
      (println "An error occurred:" (.getMessage e))
      -1)))

(defn slurp-data [{db-file :db tables-file :tables test? :test :as req}]
  (println "req" req)
  (try
    (let [{db :db} (-> db-file slurp yaml/parse-string)
          {:keys [tables root-table root-ids]} (-> tables-file slurp yaml/parse-string)
          deps (dep/build-deps-from-table-list db tables)
          table (apply u/vec-kw (str/split root-table #"\."))
          table-short (vec (take 2 table))
          {result :data} (ing/slurp-data db
                                         deps
                                         (mdb/make-dag deps table-short)
                                         (mdb/make-primary-keys deps)
                                         {table root-ids}
                                         (not test))]
      ;; TODO use clojure.pprint/print-table instead
      (doseq [[t c] (sort result)]
        (println (u/make-table-str t) "-" c))
      0)
    (catch Exception e
      (println "An error occurred:" (.getMessage e))
      (println (.getStackTrace e))
      -1)))

(def cli-config
  {:command "tenant-clone"
   :description "A cli to use dbcopy to clone a tenant worth's of data"
   :version "0.0.1"
   :opts [{:option "db"
           :type :string
           :default "db.yaml"}]
   :subcommands [{:command "test-db"
                  :description "Validates a db connection works"
                  :opts [{:option "db"
                          :type :string
                          :default "db.yaml"}]
                  :runs test-db}
                 {:command "list-tables"
                  :description "List potential tables to sync"
                  :opts [{:option "db"
                          :type :string
                          :default "db.yaml"}
                         {:option "file"
                          :short "f"
                          :type :string
                          :default "tables.yaml"}]
                  :runs list-tables}
                 {:command "slurp-data"
                  :description "Slurp data as defined in the supplied yaml setup"
                  :opts [{:option "db"
                          :type :string
                          :default "db.yaml"}
                         {:option "tables"
                          :type :string
                          :default "tables.yaml"}
                         {:option "test"
                          :type :with-flag
                          :default false
                          :short "t"}]
                  :runs slurp-data}]})

(defn -main
  "This is our entry point.
  Just pass parameters and configuration.
  Commands (functions) will be invoked as appropriate."
  [& args]
  (cli/run-cmd args cli-config))