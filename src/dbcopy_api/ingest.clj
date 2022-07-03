(ns dbcopy-api.ingest
  (:require [dbcopy-api.db :as db]
            [honey.sql.helpers :as h]
            [dbcopy-api.utils :as u]
            [clojure.set :as set]))

(defn slurp-rows [db t where-clauses]
  (cond-> (-> (h/select :*)
              (h/from (u/make-table-kw t)))
    (seq where-clauses) (h/where (apply conj [] :and where-clauses))
    :always (db/->execute db)))

(defn- make-where-clause [deps t ids]
  (map (fn [[c fc]]
         (when (seq (ids fc))
           [:in c (ids fc)]))
       (deps t)))

(defn get-records-from-recursive-table [db dep ids-to-pull]
  (let [t (first (keys dep))
        [ref-field [_ _ id-field]] (first (dep t))
        batch-size 2]
    (loop [processed-ids #{}
           todo-ids (set ids-to-pull)
           data {}]
      (if (empty? todo-ids)
        {:ids (keys data)
         :data (vals data)}
        (let [ids-now (take batch-size todo-ids)
              rows (slurp-rows db
                               t
                               [[:or
                                 [:in id-field ids-now]
                                 [:in ref-field ids-now]]])
              new-data (reduce (fn [acc r]
                                 (assoc acc (id-field r) r))
                               data
                               rows)
              ids-found (->> rows (mapcat (juxt id-field ref-field)) (filter identity) (into #{}))
              net-new-ids (set/difference ids-found processed-ids todo-ids)
              new-todos-ids (into (drop batch-size todo-ids) net-new-ids)]
          (recur (into processed-ids ids-now) new-todos-ids new-data))))))

(defn slurp-data
  ([db deps dag primary-keys seed-ids]
   (slurp-data db deps dag primary-keys seed-ids true))
  ([db deps dag primary-keys seed-ids save-rows?]
   (reduce (fn [{:keys [ids data]} t]
             (let [where-clauses (make-where-clause deps t ids)
                   rows (slurp-rows db t where-clauses)
                   new-ids (reduce (fn [acc2 pk]
                                     (assoc acc2 (conj t pk) (map pk rows)))
                                   ids
                                   (primary-keys t))
                   new-data (if save-rows?
                              (assoc data t rows)
                              (assoc data t (count rows)))]
               {:data new-data
                :ids new-ids}))
           {:data {}
            :ids seed-ids}
           (rest dag))))

;; TODO store data as we read it in

(comment
  (u/make-table-kw [:public :student])
  (slurp-rows u/yardstick-db [:public :student] [[:= :id 1]])
  (slurp-data u/yardstick-db deps dag primary-keys
              {[:public :school :id] [1]} false)
  (get-records-from-recursive-table u/yardstick-db
                                    {[:public :parent] {:parent_id [:public :parent :id]}}
                                    [8 9 10])
  ;
  )

(def slurped-data (slurp-data u/yardstick-db deps dag primary-keys
                              {[:public :school :id] [1]}
                              false))

(def deps {[:public :school_assessment_instance] {:school_id [:public :school :id]},
           [:public :student] {:school_id [:public :school :id]},
           [:public :student_assessment]
           {:student_id [:public :student :id], :school_assessment_instance_id [:public :school_assessment_instance :id]},
           [:public :student_obstacle] {:student_id [:public :student :id]},
           [:public :student_support] {:student_id [:public :student :id]},
           [:public :student_opportunity] {:student_id [:public :student :id]},
           [:public :assessment_star_v1] {:school_assessment_instance_id [:public :school_assessment_instance :id]},
           [:public :assessment_map_v1] {:school_assessment_instance_id [:public :school_assessment_instance :id]}
          ;;  [:public :parent] {:parent_id [:public :parent :id]}
           })

(def dag [[:public :school]
          [:public :school_assessment_instance]
          [:public :student]
          [:public :student_assessment]
          [:public :student_obstacle]
          [:public :student_support]
          [:public :student_opportunity]
          [:public :assessment_star_v1]
          [:public :assessment_map_v1]
          ;; [:public :parent]
          ])

(def primary-keys {[:public :school] #{:id},
                   [:public :student] #{:id},
                   [:public :school_assessment_instance] #{:id}
                  ;;  [:public :parent] #{:id}
                   })
