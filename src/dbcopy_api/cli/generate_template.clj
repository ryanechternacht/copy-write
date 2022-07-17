(ns dbcopy-api.cli.generate-template
  (:require [clj-yaml.core :as yaml]
            [clojure.java.io :as io]
            [dbcopy-api.dependencies :as dep]))

(defn- read-files-from-folder
  "Given a folder slurps and `read-strings` every file in it"
  [folder]
  (->> (io/file folder)
       file-seq
       (filter #(.isFile %))
       (map slurp)
       (map read-string)))

;; TODO is there a prettier printing form for yaml?
;;   - ideally, we'd put newlines between top level elems
;;   - ideally, we'd print blank for nil (now it prints null, so we use "")
(defn generate-template [{template-file :file}]
  (let [db-specs (read-files-from-folder ".dbcopy/dbs")
        tables (reduce (fn [m spec]
                         (assoc m
                                (:nickname spec)
                                (map :table (dep/get-all-tables spec))))
                       {}
                       db-specs)]
    (->> {:version "v0.1"
          :root-table {:db ""
                       :schema ""
                       :table ""}
          :root-ids {:col ""
                     :values ""}
          :tables tables
          :cross-db [{:db1 "" :schema1 "" :table1 "" :col1 ""
                      :db2 "" :schema2 "" :table2 "" :col2 ""}]}
         (#(yaml/generate-string % :dumper-options {:flow-style :block}))
         (spit template-file))
    (println "Generated template at" template-file)))
