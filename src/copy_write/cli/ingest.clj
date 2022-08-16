(ns copy-write.cli.ingest
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clj-yaml.core :as yaml]
            [copy-write.ingest :as ing]
            [copy-write.dependencies :as deps]
            [copy-write.map-db :as mdb]
            [copy-write.utils :as u]))

(defn ingest-v01 [file test?]
  ;; TODO handle multiple dbs
  (let [db (first (u/read-files-from-folder ".copy-write/dbs"))
        {:keys [tables root-table root-id]} (-> file slurp yaml/parse-string)
        ;; TODO pull the correct db here
        deps (deps/build-deps-from-table-list db (:yardstick tables))
        table (u/vec-kw (:schema root-table) (:table root-table) (:col root-id))
        table-short (u/vec-kw (:schema root-table) (:table root-table))
        ingested-data (ing/slurp-data db
                                      deps
                                      (mdb/make-dag deps table-short)
                                      (mdb/make-primary-keys deps)
                                      {table [(:value root-id)]}
                                      (not test?))]
    (if test?
      ;; TODO use clojure.pprint/print-table instead
      (doseq [[t c] (sort (:data ingested-data))]
        (println "Testing ingest")
        (println (u/make-table-str t) "-" c))
      (let [run-name (subs file 0 (str/last-index-of file ".yaml"))
            folder (str ".copy-write/ingested/" run-name "/")]
        (println "Ingesting Data")
        ;;  necessary because if we just grab the whole obj, we don't realize
        ;; the lazy-seqs that are tables and root-ids
        ;; TODO do we want this cacheing?
        (io/make-parents (str folder "temp-file"))
        (spit (str folder "_deps.edn") deps)
        (spit (str folder "_setup.edn") {:tables tables
                                         :root-table root-table
                                         :root-id root-id})
        (spit (str folder "_ids.edn") (:ids ingested-data))
        ;; TODO don't require this
        (spit (str folder "_slurped-data.edn") ingested-data)
        (doseq [[t d] (:data ingested-data)]
          ;; TODO actual path handling library
          (spit (str folder (u/make-table-str t) ".edn") d)
          (println (u/make-table-str t) "-" (count d)))))))

(defn ingest [{:keys [file test]}]
  (let [{:keys [version]} (-> file slurp yaml/parse-string)]
    (condp = version
      "v0.1" (ingest-v01 file test))))
