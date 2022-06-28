(ns dbcopy-api.egest
  (:require [dbcopy-api.db :as db]
            [honey.sql.helpers :as h]
            [dbcopy-api.utils :as u]
            [com.rpl.specter :as s]
            [clojure.set :as set]
            [dbcopy-api.ingest :as ing]))

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
      (-> (h/insert-into (u/make-table-kw t))
          (h/values rows)
          (h/returning :*)))))

(defn insert-root-row [db table-col cols original-id]
  (let [row (reduce (fn [acc {:keys [column value]}]
                      (assoc acc (keyword column) value))
                    {}
                    cols)
        ;; insert-cols (map :column cols)
        [s t c] (map keyword table-col)
        ;;  TODO accept more than one id here
        new-row (-> (h/insert-into (u/make-table-kw [s t]))
                    (h/values [row])
                    (h/returning :*)
                    (db/->execute db))]
    {:seed-values {[s t] new-row}
     :new-ids {[s t c] {original-id (first (map c new-row))}}}))

(defn insert-rows [db deps dag primary-keys {:keys [ids data]} seed-data new-ids]
  (loop [[t & others] (rest dag)
         new-ids new-ids
         spat-data seed-data]
    (if (nil? t)
      spat-data
      (let [new-insert-sql (build-insert deps data t new-ids)]
        (if (nil? new-insert-sql)
          (recur others new-ids spat-data)
          (let [new-rows (db/execute db new-insert-sql)
               ;; TODO hanlde multiple ids
                col (first (primary-keys t))]
            (recur others
                   (if col
                     (let [full-col (conj t col)
                           mapped-ids (zipmap (ids full-col) (map col new-rows))
                           new-ids (assoc new-ids full-col mapped-ids)]
                       new-ids)
                     new-ids)
                   (assoc spat-data t new-rows))))))))

(comment
  (insert-rows u/yardstick-db
               ing/deps
               ing/dag
               ing/primary-keys
               ing/slurped-data
               {[:public :school] [{:id 2
                                    :name "Ridge View Elementary"}]})
  ;
  )

