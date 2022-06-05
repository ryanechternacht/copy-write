(ns dbcopy-api.egest
  (:require [yardstick-api.db :as db]
            [honey.sql.helpers :as h]
            [work.helpers :as wh]
            [com.rpl.specter :as s]
            [clojure.set :as set]
            [work.ingest :as ing]))

;; I should be able to pull the :id from cols or something
(defn- drop-keys [rec]
 (dissoc rec :id :updated_at :created_at))
 
(defn- get-new-keys [rec deps t new-ids]
 (let [cols (deps t)]
   (reduce (fn [r [c fc]]
             (assoc r c ((new-ids fc) (r c))))
           rec
           cols)))

;; we should handle jsonb cols (I thought code in db.clj should
;; alredy do this, so I'm not sure why it's not)
(defn- escape-additional-fields [{:keys [additional_fields] :as rec}]
  (if additional_fields
    (assoc rec :additional_fields nil)
    rec))

(defn- prep-data [deps rec t new-ids]
 (-> rec
     drop-keys
     escape-additional-fields
     (get-new-keys deps t new-ids)))

(defn- build-insert [deps data t new-ids]
 (let [rows (map #(prep-data deps % t new-ids) (data t))]
   (when (seq rows)
     (-> (h/insert-into (wh/make-table-kw t))
         (h/values rows)
         (h/returning :*)))))

(defn insert-rows [db deps dag primary-keys {:keys [ids data]} seed-data]
  (loop [[t & others] (rest dag)
         new-ids {[:public :school :id] {1 2}}
         spat-data seed-data]
    (if (nil? t)
      spat-data
      (do
        (println t)
        (let [new-insert-sql (build-insert deps data t new-ids)]
          (if (nil? new-insert-sql)
            (recur others new-ids spat-data)
            (let [new-rows (db/execute db new-insert-sql)
               ;; can't stay as first
                  col (first (primary-keys t))]
              (recur others
                     (if col
                       (let [full-col (conj t col)
                             mapped-ids (zipmap (ids full-col) (map col new-rows))
                             new-ids (assoc new-ids full-col mapped-ids)]
                         new-ids)
                       new-ids)
                     (assoc spat-data t new-rows)))))))))

(comment
  (insert-rows wh/yardstick-db
               ing/deps
               ing/dag
               ing/primary-keys
               ing/slurped-data
               {[:public :school] [{:id 2
                                    :name "Ridge View Elementary"}]})
  ;
  )

