(ns dbcopy-api.map-db
  (:require [com.rpl.specter :as s]))

;; all this needs to do is map k to the set of value's value's 
;; and remove the 3rd element of each value's values
;; (defn make-deps-slim [deps]
;;   (into {}
;;         (map (fn [[k vs]]
;;                [k (into #{} (map (fn [[_ vs2]] (->> vs2
;;                                                      (take 2)
;;                                                      vec))
;;                                  vs))])
;;              deps)))

;; specter implementation of the above
(defn make-deps-slim [deps]
  (s/transform
   [s/MAP-VALS (s/collect s/MAP-VALS (s/srange 0 2))]
   (fn [collected _] (into #{} collected))
   deps))

(defn make-dag [deps starting-table]
  (loop [[[t deps] & others] (make-deps-slim deps)
         resolved [starting-table]]
    (if (nil? t)
      resolved
      (let [resolved-set (set resolved)
            has-deps-set (set (map first others))]
        (if (every? #(or (resolved-set %) (not (has-deps-set %))) deps)
          (recur others (conj (apply conj resolved (filter #(not (resolved-set %)) deps)) t))
          (recur (conj (vec others) [t deps]) resolved))))))

(defn make-primary-keys [deps]
  (reduce (fn [acc [_ m]]
            (reduce (fn [acc2 [_ col]]
                      (let [t (vec (take 2 col))
                            c (nth col 2)]
                        (assoc acc2 t (conj (get acc2 t #{}) c))))
                    acc
                    m))
          {}
          deps))

(comment
  (make-deps-slim deps)
  (make-dag deps [:public :school])
  (make-dag-2 deps [:public :school])
  (make-primary-keys deps)
  ;
  (def deps-2
    (-> deps
        (update-in [[:public :school_assessment_instance]] assoc :support_id [:public :student_support :id])
        (update-in [[:public :school_opportunity]] assoc :a [:b :c :d])))



  (def deps {[:public :school_assessment_instance] {:school_id [:public :school :id]},
             [:public :student] {:school_id [:public :school :id]},
             [:public :student_assessment]
             {:student_id [:public :student :id], :school_assessment_instance_id [:public :school_assessment_instance :id]},
             [:public :student_obstacle] {:student_id [:public :student :id]},
             [:public :student_support] {:student_id [:public :student :id]},
             [:public :student_opportunity] {:student_id [:public :student :id]},
             [:public :assessment_star_v1] {:school_assessment_instance_id [:public :school_assessment_instance :id]},
             [:public :assessment_map_v1] {:school_assessment_instance_id [:public :school_assessment_instance :id]}
             [:public :parent] {:parent_id [:public :parent :id]
                                :school_id [:public :school :id]}})


;; what is this?
  (map (fn [x]
         {:table x
          :type :normal})
       [[:public :school]
        [:public :school_assessment_instance]
        [:public :parent]
        [:public :parent]
        [:public :assessment_star_v1]
        [:public :student]
        [:public :student_assessment]
        [:public :assessment_map_v1]
        [:public :student_obstacle]
        [:public :student_opportunity]
        [:public :student_support]])
  ;
  )
