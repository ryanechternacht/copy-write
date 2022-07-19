(ns dbcopy-api
  (:require [cli-matic.core :as cli]
            [clojure.string :as str]
            [clj-yaml.core :as yaml]
            [dbcopy-api.cli.add-db :as cli-add-db]
            [dbcopy-api.cli.generate-template :as cli-gen-t]
            [dbcopy-api.cli.ingest :as cli-ing]
            [dbcopy-api.cli.egest :as cli-eg]
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

;; copy cat
;; kopy kat
;; george cloney
;; copy write

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
                  :runs cli-ing/ingest}
                 {:command "egest"
                  :description "Creates a copy of a dataset that was pulled before"
                  :opts (conj shared-opts
                              {:option "dataset"
                               :short "d"
                               :type :string
                               :description "The name of the file template file (with or without '.yaml')"
                               :default "template"}
                              {:option "result"
                               :short "r"
                               :type :string
                               :description "File outling the results of the run"
                               :default "result.yaml"})
                  :runs cli-eg/egest}]})

(defn -main
  "This is our entry point.
  Just pass parameters and configuration.
  Commands (functions) will be invoked as appropriate."
  [& args]
  (cli/run-cmd args cli-config))
