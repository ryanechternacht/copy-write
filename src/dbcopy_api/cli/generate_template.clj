(ns dbcopy-api.cli.generate-template
  (:require [clj-yaml.core :as yaml]
            [dbcopy-api.dependencies :as dep]))

;; TODO is there a prettier printing form for yaml?
;;   - ideally, we'd put newlines between top level elems
;;   - ideally, we'd print blank for nil (now it prints null, so we use "")
(defn generate-template [{template-file :file}]
  (let [;; TODO pull from folder (need internet)
        db-files [".dbcopy/dbs/yardstick.edn" ".dbcopy/dbs/ys.edn"]
        tables (reduce (fn [m f]
                         (let [spec (read-string (slurp f))]
                           (assoc m
                                  (:nickname spec)
                                  (map :table (dep/get-all-tables spec)))))
                       {}
                       db-files)]
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

