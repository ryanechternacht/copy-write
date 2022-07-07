(ns dbcopy-api
  (:require [cli-matic.core :as cli]
            [clojure.string :as str]
            [clj-yaml.core :as yaml]
            [dbcopy-api.db :as db]
            [dbcopy-api.dependencies :as dep]
            [dbcopy-api.egest :as eg]
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

(defn slurp-data [{db-file :db tables-file :tables test? :test output-folder :output}]
  (try
    ;; TODO we should save out deps (or more?)
    (let [{db :db} (-> db-file slurp yaml/parse-string)
          {:keys [tables root-table root-ids]}
          (-> tables-file slurp yaml/parse-string)
          deps (dep/build-deps-from-table-list db tables)
          table (apply u/vec-kw (str/split root-table #"\."))
          table-short (vec (take 2 table))
          slurped-data (ing/slurp-data db
                                       deps
                                       (mdb/make-dag deps table-short)
                                       (mdb/make-primary-keys deps)
                                       {table root-ids}
                                       (not test?))]
      (if test?
      ;; TODO use clojure.pprint/print-table instead
        (doseq [[t c] (sort (:data slurped-data))]
          (println (u/make-table-str t) "-" c))
        (do
          (spit (str output-folder "/_deps.edn") deps)
          ;; necessary because if we just grab the whole obj, we don't realize
          ;; the lazy-seqs that are tables and root-ids
          (spit (str output-folder "/_tables.edn") {:tables tables
                                                    :root-table root-table
                                                    :root-ids root-ids})
          (spit (str output-folder "/_slurped-data.edn") slurped-data)
          (doseq [[t d] (:data slurped-data)]
          ;; TODO actul path handling library
            (spit (str output-folder "/" (u/make-table-str t) ".edn") d)
            (println (u/make-table-str t) "-" (count d)))))
      0)
    (catch Exception e
      (println "An error occurred:" (.getMessage e))
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

;; TODO some global fn to flush *out*?
;; TODO -v to turn on stack traces
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
                          :short "t"
                          :type :with-flag
                          :default false}
                         {:option "output"
                          :short "o"
                          :type :string
                          :default "slurped-data"}]
                  :runs slurp-data}
                 {:command "spit-data"
                  :description "Spit stored data"
                  :opts [{:option "db"
                          :type :string
                          :default "db.yaml"}
                         {:option "slurped-data"
                          :short "d"
                          :type :string
                          :default "slurped-data"}
                         {:option "tables"
                          :type :string
                          :default "tables.yaml"}]
                  :runs spit-data}]})

(defn -main
  "This is our entry point.
  Just pass parameters and configuration.
  Commands (functions) will be invoked as appropriate."
  [& args]
  (cli/run-cmd args cli-config))