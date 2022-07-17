(ns dbcopy-api.cli.ingest
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clj-yaml.core :as yaml]
            [dbcopy-api.ingest :as ing]
            [dbcopy-api.dependencies :as deps]
            [dbcopy-api.map-db :as mdb]
            [dbcopy-api.utils :as u]))

;; TODO copy-paste
(defn- read-files-from-folder
  "Given a folder slurps and `read-strings` every file in it"
  [folder]
  (->> (io/file folder)
       file-seq
       (filter #(.isFile %))
       (map slurp)
       (map read-string)))

(defn ingest-v01 [file test?]
  ;; TODO handle multiple dbs
  (let [db (first (read-files-from-folder ".dbcopy/dbs"))
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
        (println (u/make-table-str t) "-" c))
      (do
        ;; TODO do we want this cacheing?
        ;; (spit (str output-folder "/_deps.edn") deps)
        ;; necessary because if we just grab the whole obj, we don't realize
        ;; the lazy-seqs that are tables and root-ids
        ;; (spit (str output-folder "/_tables.edn") {:tables tables
        ;;                                           :root-table root-table
        ;;                                           :root-ids root-ids})
        ;; (spit (str output-folder "/_slurped-data.edn") slurped-data)
        (let [run-name (subs file 0 (str/last-index-of file ".yaml"))
              folder (str ".dbcopy/ingested/" run-name "/")]
          (io/make-parents (str folder "temp-file"))
          (doseq [[t d] (:data ingested-data)]
          ;; TODO actual path handling library
            (spit (str folder (u/make-table-str t) ".edn") d)
            (println (u/make-table-str t) "-" (count d))))))))

;; (str/last-index-of "boo.yaml" ".yaml")

(defn ingest [{:keys [file test]}]
  (let [{:keys [version]} (-> file slurp yaml/parse-string)]
    (condp = version
      "v0.1" (ingest-v01 file test))))

;; (spit ".dbcopy/new-folder/hello" "hello world")

;; (io/make-parents ".dbcopy/a/b/c")