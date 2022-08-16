(ns dbcopy-api.cli.egest
  (:require [clojure.string :as str]
            [clj-yaml.core :as yaml]
            [dbcopy-api.egest :as eg]
            [dbcopy-api.map-db :as mdb]
            [dbcopy-api.utils :as u]))

(defn egest [{:keys [dataset result]}]
  ;; TODO handle multiple dbs
  (let [db (first (u/read-files-from-folder ".dbcopy/dbs"))
        yaml-loc (str/last-index-of dataset ".yaml")
        dataset-name (if yaml-loc (subs dataset 0 yaml-loc) dataset)
        folder (str ".dbcopy/ingested/" dataset-name "/")
        {:keys [root-table root-id]}
        (-> (str folder "_setup.edn") slurp read-string)
        deps (-> (str folder "_deps.edn") slurp read-string)
        table (u/vec-kw (:schema root-table) (:table root-table) (:col root-id))
        table-short (u/vec-kw (:schema root-table) (:table root-table))
        ;; TODO where does this come from?
        body [{:column "name" :value "Ridge View Elementary"}]
        {:keys [seed-values new-ids]} (eg/insert-root-row db
                                                          table
                                                          body
                                                          (:value root-id))
        outcome (eg/insert-rows db
                                deps
                                (mdb/make-dag deps table-short)
                                (mdb/make-primary-keys deps)
                                folder
                                {table-short seed-values}
                                new-ids)]
    (println "Generated new data.")
    ;; TODO reformat this file a bit so the new id is easier to grab
    (spit result (yaml/generate-string {:root-table root-table
                                        :new-id {:col (:col root-id)
                                                 :value (first new-ids)}}))
    (printf "Table: %s -- id: %s\n" (u/make-table-str table) (->> new-ids first second first second))
    (printf "Wrote %d new records across %d tables\n" (->> outcome (map second) (map count) (reduce +)) (count outcome))
    (printf "See `%s` for more details\n" result)))
