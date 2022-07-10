(ns dbcopy-api.dependencies
  (:require [clojure.string :as str]
            [com.rpl.specter :as s]
            [dbcopy-api.db :as db]
            [dbcopy-api.utils :as u]
            [honey.sql.helpers :as h]))

(def cols
  [;; {:from_schema "public",
  ;;   :from_table "pronouns",
  ;;   :from_column "accusative_upper_lang",
  ;;   :to_schema "public",
  ;;   :to_table "language_lookup",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "pronouns",
  ;;   :from_column "accusative_lang",
  ;;   :to_schema "public",
  ;;   :to_table "language_lookup",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "pronouns",
  ;;   :from_column "possessive_upper_lang",
  ;;   :to_schema "public",
  ;;   :to_table "language_lookup",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "pronouns",
  ;;   :from_column "possessive_lang",
  ;;   :to_schema "public",
  ;;   :to_table "language_lookup",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "pronouns",
  ;;   :from_column "nominative_upper_lang",
  ;;   :to_schema "public",
  ;;   :to_table "language_lookup",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "pronouns",
  ;;   :from_column "nominative_lang",
  ;;   :to_schema "public",
  ;;   :to_table "language_lookup",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "student",
  ;;   :from_column "pronouns_id",
  ;;   :to_schema "public",
  ;;   :to_table "pronouns",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "student_assessment",
  ;;   :from_column "grade_id",
  ;;   :to_schema "public",
  ;;   :to_table "grade",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "student",
  ;;   :from_column "grade_id",
  ;;   :to_schema "public",
  ;;   :to_table "grade",
  ;;   :to_column "id"}
   {:from_schema "public",
    :from_table "school_assessment_instance",
    :from_column "school_id",
    :to_schema "public",
    :to_table "school",
    :to_column "id"}
   {:from_schema "public",
    :from_table "student",
    :from_column "school_id",
    :to_schema "public",
    :to_table "school",
    :to_column "id"}
   {:from_schema "public",
    :from_table "student_assessment",
    :from_column "student_id",
    :to_schema "public",
    :to_table "student",
    :to_column "id"}
   {:from_schema "public",
    :from_table "student_obstacle",
    :from_column "student_id",
    :to_schema "public",
    :to_table "student",
    :to_column "id"}
   {:from_schema "public",
    :from_table "student_support",
    :from_column "student_id",
    :to_schema "public",
    :to_table "student",
    :to_column "id"}
   {:from_schema "public",
    :from_table "student_opportunity",
    :from_column "student_id",
    :to_schema "public",
    :to_table "student",
    :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "student_opportunity",
  ;;   :from_column "opportunity_id",
  ;;   :to_schema "public",
  ;;   :to_table "opportunity",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "support_support_step",
  ;;   :from_column "support_id",
  ;;   :to_schema "public",
  ;;   :to_table "support",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "support_support_tag",
  ;;   :from_column "support_id",
  ;;   :to_schema "public",
  ;;   :to_table "support",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "student_support",
  ;;   :from_column "support_id",
  ;;   :to_schema "public",
  ;;   :to_table "support",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "support_support_tag",
  ;;   :from_column "support_tag_id",
  ;;   :to_schema "public",
  ;;   :to_table "support_tag",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "support_support_step",
  ;;   :from_column "support_step_id",
  ;;   :to_schema "public",
  ;;   :to_table "support_step",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "student_obstacle",
  ;;   :from_column "obstacle_id",
  ;;   :to_schema "public",
  ;;   :to_table "obstacle",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "yardstick_grant",
  ;;   :from_column "user_id",
  ;;   :to_schema "public",
  ;;   :to_table "yardstick_user",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "school_assessment_instance",
  ;;   :from_column "academic_year_id",
  ;;   :to_schema "public",
  ;;   :to_table "academic_year",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "assessment_assessment_trait",
  ;;   :from_column "assessment_id",
  ;;   :to_schema "public",
  ;;   :to_table "assessment",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "assessment_period",
  ;;   :from_column "assessment_id",
  ;;   :to_schema "public",
  ;;   :to_table "assessment",
  ;;   :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "school_assessment_instance",
  ;;   :from_column "assessment_period_id",
  ;;   :to_schema "public",
  ;;   :to_table "assessment_period",
  ;;   :to_column "id"}
   {:from_schema "public",
    :from_table "assessment_star_v1",
    :from_column "school_assessment_instance_id",
    :to_schema "public",
    :to_table "school_assessment_instance",
    :to_column "id"}
   {:from_schema "public",
    :from_table "assessment_map_v1",
    :from_column "school_assessment_instance_id",
    :to_schema "public",
    :to_table "school_assessment_instance",
    :to_column "id"}
   {:from_schema "public",
    :from_table "student_assessment",
    :from_column "school_assessment_instance_id",
    :to_schema "public",
    :to_table "school_assessment_instance",
    :to_column "id"}
  ;;  {:from_schema "public",
  ;;   :from_table "assessment_assessment_trait",
  ;;   :from_column "assessment_trait_id",
  ;;   :to_schema "public",
  ;;   :to_table "assessment_trait",
  ;;   :to_column "id"}
   ])

;; (defn get-referenced-cols [db]
;;   (-> (h/select [:kcu1.table_schema :from_schema]
;;                 [:kcu1.table_name :from_table]
;;                 [:kcu1.column_name :from_column]
;;                 [:kcu2.table_schema :to_schema]
;;                 [:kcu2.table_name :to_table]
;;                 [:kcu2.column_name :to_column])
;;       (h/from [:information_schema.referential_constraints :rc])
;;       (h/join [:information_schema.key_column_usage :kcu1]
;;               [:and
;;                [:= :rc.constraint_catalog :kcu1.constraint_catalog]
;;                [:= :rc.constraint_schema :kcu1.constraint_schema]
;;                [:= :rc.constraint_name :kcu1.constraint_name]])
;;       (h/join [:information_schema.key_column_usage :kcu2]
;;               [:and
;;                [:= :rc.unique_constraint_catalog :kcu2.constraint_catalog]
;;                [:= :rc.unique_constraint_schema :kcu2.constraint_schema]
;;                [:= :rc.unique_constraint_name :kcu2.constraint_name]])
;;       (db/->execute db)))

(defn make-table-list-where [filter-tables]
  (if (empty? filter-tables)
    true
    (let [tuples (map #(str/split % #"\.") filter-tables)]
      [:and
       [:in [:composite :kcu1.table_schema :kcu1.table_name] tuples]
       [:in [:composite :kcu2.table_schema :kcu2.table_name] tuples]])))

(db/->format
 (h/where {}
          (make-table-list-where
           '("public.school" "public.support" "public.assessment_map_v1" "public.student_obstacle" "public.assessment_star_v1" "public.student_opportunity" "public.school_assessment_instance" "public.student_assessment" "public.student_support" "public.student"))))

(defn get-referenced-cols
  ([db] (get-referenced-cols db nil))
  ([db filter-tables]
   (-> (h/select [:kcu1.table_schema :from_schema]
                 [:kcu1.table_name :from_table]
                 [:kcu1.column_name :from_column]
                 [:kcu2.table_schema :to_schema]
                 [:kcu2.table_name :to_table]
                 [:kcu2.column_name :to_column])
       (h/from [:information_schema.referential_constraints :rc])
       (h/join [:information_schema.key_column_usage :kcu1]
               [:and
                [:= :rc.constraint_catalog :kcu1.constraint_catalog]
                [:= :rc.constraint_schema :kcu1.constraint_schema]
                [:= :rc.constraint_name :kcu1.constraint_name]])
       (h/join [:information_schema.key_column_usage :kcu2]
               [:and
                [:= :rc.unique_constraint_catalog :kcu2.constraint_catalog]
                [:= :rc.unique_constraint_schema :kcu2.constraint_schema]
                [:= :rc.unique_constraint_name :kcu2.constraint_name]])
       (h/where (make-table-list-where filter-tables))
       (db/->execute db))))

(defn get-all-cols [db]
  (->> (-> (h/select :table_schema :table_name :column_name)
           (h/from :information_schema.columns)
           (h/where :not-in :table_schema '("pg_catalog" "information_schema"))
           (db/->execute db))
       (map (fn [{:keys [table_schema table_name column_name]}]
              {:table (str table_schema "." table_name)
               :column column_name}))))

;; TODO does this need to return [{:table "xxx"}, ...] instead of just ["xxx", ...]
(defn get-all-tables [db]
  (->> (-> (h/select :table_schema :table_name)
           (h/from :information_schema.tables)
           (h/where :not-in :table_schema '("pg_catalog" "information_schema"))
           (db/->execute db))
       (map (fn [{:keys [table_schema table_name]}]
              {:table (str table_schema "." table_name)}))))

(defn get-primary-key-cols [db table-col]
  (let [[s t] (clojure.string/split table-col #"\.")]
    (->> (-> (h/select :key_column_usage.column_name)
             (h/from :information_schema.table_constraints)
             (h/join :information_schema.key_column_usage
                     [:and
                      [:=
                       :table_constraints.table_catalog
                       :key_column_usage.table_catalog]
                      [:=
                       :table_constraints.table_schema
                       :key_column_usage.table_schema]
                      [:=
                       :table_constraints.table_name
                       :key_column_usage.table_name]
                      [:=
                       :table_constraints.constraint_name
                       :key_column_usage.constraint_name]])
             (h/where [:and
                       [:= :table_constraints.constraint_type "PRIMARY KEY"]
                       [:= :table_constraints.table_schema s]
                       [:= :table_constraints.table_name t]])
             (db/->execute db))
         (map :column_name))))

(defn get-table-columns [db table]
  (let [[s t] (clojure.string/split table #"\.")]
    (-> (h/select :columns.column_name
                  [[:case
                    [:and
                     [:= :table_constraints.constraint_type "PRIMARY KEY"]
                     [:is :referential_constraints.constraint_name nil]]
                    "PRIMARY KEY"
                    [:and
                     [:= :table_constraints.constraint_type "PRIMARY KEY"]
                     [:is-not :referential_constraints.constraint_name nil]]
                    "FOREIGN KEY"]
                   :key_type])
        (h/from :information_schema.columns)
        (h/left-join :information_schema.key_column_usage
                     [:and
                      [:=
                       :columns.table_catalog
                       :key_column_usage.table_catalog]
                      [:=
                       :columns.table_schema
                       :key_column_usage.table_schema]
                      [:=
                       :columns.table_name
                       :key_column_usage.table_name]
                      [:=
                       :columns.column_name
                       :key_column_usage.column_name]])
        (h/left-join :information_schema.table_constraints
                     [:and
                      [:=
                       :key_column_usage.table_catalog
                       :table_constraints.table_catalog]
                      [:=
                       :key_column_usage.table_schema
                       :table_constraints.table_schema]
                      [:=
                       :key_column_usage.table_name
                       :table_constraints.table_name]
                      [:=
                       :table_constraints.constraint_type
                       "PRIMARY KEY"]])
        (h/left-join :information_schema.referential_constraints
                     [:and
                      [:= :referential_constraints.constraint_catalog :key_column_usage.constraint_catalog]
                      [:= :referential_constraints.constraint_schema :key_column_usage.constraint_schema]
                      [:= :referential_constraints.constraint_name :key_column_usage.constraint_name]])
        (h/where [:and
                  [:= :columns.table_schema s]
                  [:= :columns.table_name t]])
        (db/->execute db))))

(defn build-deps [cols]
  (reduce (fn [acc {:keys [from_schema from_table from_column to_schema to_table to_column]}]
            (update acc
                    (u/vec-kw from_schema from_table)
                    assoc
                    (keyword from_column)
                    (u/vec-kw to_schema to_table to_column)))
          {}
          cols))

(defn build-deps-from-table-list [db tables]
  (->> (get-referenced-cols db tables)
       (build-deps)))

(defn deps->json [deps]
  (into [] (map (fn [[k v]]
                  [(u/make-table-kw k)
                   (into {} (map (fn [[k2 v2]]
                                   [k2 [(u/make-table-kw (take 2 v2)) (nth v2 2)]]) v))])
                deps)))

(defn json-table->kw [t]
  (vec (map keyword (clojure.string/split (name t) #"\."))))

(defn json->deps [json]
  (->> json
       (s/transform
        [s/MAP-VALS s/MAP-VALS]
        (fn [[t c]]
          (conj (json-table->kw t) (keyword c))))
       (s/transform
        [s/MAP-KEYS]
        json-table->kw)))

(comment
  (get-all-cols u/yardstick-db)
  (get-referenced-cols u/yardstick-db)
  (get-primary-key-cols u/yardstick-db "public.student")
  (get-table-columns u/yardstick-db "public.grade")
  (build-deps cols)
  (deps->json {[:public :school_assessment_instance] {:school_id [:public :school :id]}})
  ;
  )
