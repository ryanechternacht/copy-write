(ns copy-write
  (:require [cli-matic.core :as cli]
            [copy-write.cli.add-db :as cli-add-db]
            [copy-write.cli.generate-template :as cli-gen-t]
            [copy-write.cli.ingest :as cli-ing]
            [copy-write.cli.egest :as cli-eg]))

;; clj -M -m copy-write test-db --db=db.yaml

(defmacro with-harness [f]
  `(fn [cli-opts#]
     (try
       (~f cli-opts#)
       0 ;; successful return code
       (catch Exception e#
         (println "An error occured:" (.getMessage e#))
         (when (:verbose cli-opts#)
           (.printStackTrace e#))
         -1 ;; failed status code
         )
       (finally (flush)))))

;; TODO can the global flags be used anywhere? (and not require this)
(def shared-opts [{:option "verbose"
                   :short "v"
                   :type :with-flag
                   :default false}])

(def cli-config
  {:command "copy-write"
   :description "A cli to use copy-write to clone a tenant worth's of data"
   :version "0.0.1"
   :opts shared-opts
   :subcommands [{:command "add-db"
                  :description "Adds a database. Use one of the flags to select which"
                  :opts (conj shared-opts
                              {:option "postgres"
                               :short "p"
                               :type :with-flag
                               :default false})
                  :runs (with-harness cli-add-db/add-db)}
                 {:command "gen-template"
                  :description "Generates a template for a new pull from the dbs added"
                  ;; TODO add the ability to filter dbs now?
                  :opts (conj shared-opts
                              {:option "file"
                               :short "f"
                               :type :string
                               :default "template.yaml"})
                  :runs (with-harness cli-gen-t/generate-template)}
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
                  :runs (with-harness cli-ing/ingest)}
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
                  :runs (with-harness cli-eg/egest)}]})

(defn -main
  "This is our entry point.
  Just pass parameters and configuration.
  Commands (functions) will be invoked as appropriate."
  [& args]
  (cli/run-cmd args cli-config))
