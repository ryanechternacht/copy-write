(ns dbcopy-api
  (:require [clj-yaml.core :as yaml]
            [cli-matic.core :as cli]
            [dbcopy-api.db :as db]
            [dbcopy-api.dependencies :as dep]
            [honey.sql.helpers :as h]))

;; clj -M -m dbcopy-api test-db --db=db.yaml

(yaml/parse-string (slurp "test.yaml"))

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
      (->> {:tables tables}
           (#(yaml/generate-string % :dumper-options {:flow-style :block}))
           (spit tables-file))
      (println "Generated table list at" tables-file)
      0)
    (catch Exception e
      (println "An error occurred:" (.getMessage e))
      -1)))

(def cli-config
  {:command "tenant-clone"
   :description "A cli to use dbcopy to clone a tenant worth's of data"
   :version     "0.0.1"
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
                  :runs list-tables}]})

(defn -main
  "This is our entry point.
  Just pass parameters and configuration.
  Commands (functions) will be invoked as appropriate."
  [& args]
  (cli/run-cmd args cli-config))