(ns dbcopy-api
  (:require [cli-matic.core :as cli]
            [clojure.string :as str]
            [clj-yaml.core :as yaml]
            [dbcopy-api.cli.add-db :as cli-add-db]
            [dbcopy-api.cli.generate-template :as cli-gen-t]
            [dbcopy-api.cli.ingest :as cli-ing]
            [dbcopy-api.db :as db]
            [dbcopy-api.egest :as eg]
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

(defn spit-data [{db-file :db  slurped-data-folder :slurped-data}]
  (try
    (let [{db :db} (-> db-file slurp yaml/parse-string)
          {:keys [root-table root-ids]}
          (-> (str slurped-data-folder "/_tables.edn") slurp read-string)
          deps (-> (str slurped-data-folder "/_deps.edn") slurp read-string)
          slurped-data (-> (str slurped-data-folder "/_slurped-data.edn") slurp read-string)
          table (apply u/vec-kw (str/split root-table #"\."))
          table-kw (vec (take 2 table))
          ;; TODO where does this come from?
          body [{:column "name" :value "Ridge View Elementary"}]
          {:keys [seed-values new-ids]} (eg/insert-root-row db
                                                            table
                                                            body
                                                            (first root-ids))
          outcome (eg/insert-rows db
                                  deps
                                  (mdb/make-dag deps table-kw)
                                  (mdb/make-primary-keys deps)
                                  slurped-data
                                  {table-kw seed-values}
                                  new-ids)]
      ;; TODO spit 1 table at a time
      (println "Generated new data.")
      (printf "Table: %s -- id: %s\n" (u/make-table-str table) (->> new-ids first second first second))
      (printf "Wrote %d new records across %d tables\n" (->> outcome (map second) (map count) (reduce +)) (count outcome))
      (flush)
      0)
    (catch Exception e
      (println "An error occurred:" (.getMessage e))
      -1)))


;; TODO I forget how to write macros
(defmacro handle-errors [f {:keys [verbose] :as cli-opts}]
  ~(fn [cli-opts]
     (try
       (f cli-opts)
       (flush)
       0 ;; successful return code
       (catch Exception e
         (println "An error occurred:" (.getMessage e))
         (when verbose
           (.printStackTrace e))
         -1))))

;; TODO can the global flags be used anywhere? (and not require this)
(def shared-opts [{:option "verbose"
                   :short "v"
                   :type :with-flag
                   :default false}])

(def cli-config
  {:command "dbcopy"
   :description "A cli to use dbcopy to clone a tenant worth's of data"
   :version "0.0.1"
   :opts shared-opts
   :subcommands [{:command "add-db"
                  :description "Adds a database. Use one of the flags to select which"
                  :opts (conj shared-opts
                              {:option "postgres"
                               :short "p"
                               :type :with-flag
                               :default false})
                  :runs cli-add-db/add-db}
                 {:command "gen-template"
                  :description "Generates a template for a new pull from the dbs added"
                  ;; TODO add the ability to filter dbs now?
                  :opts (conj shared-opts
                              {:option "file"
                               :short "f"
                               :type :string
                               :default "template.yaml"})
                  :runs cli-gen-t/generate-template}
                 {:command "ingest"
                  :description "Ingests the data outlined by a template yaml file"
                  :opts (conj shared-opts
                              {:option "file"
                               :short "f"
                               :type :string
                               :default "template.yaml"}
                              {:option "test"
                               :short "t"
                               :type :with-flag
                               :default false})
                  :runs cli-ing/ingest}]})

(defn -main
  "This is our entry point.
  Just pass parameters and configuration.
  Commands (functions) will be invoked as appropriate."
  [& args]
  (cli/run-cmd args cli-config))
