(ns dbcopy-api.dependencies
  (:require [dbcopy-api.db :as db]
            [honey.sql.helpers :as h]
            [dbcopy-api.utils :as u]))

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

(defn get-referenced-cols [db]
  (-> (h/select [:kcu1.table_schema :from_schema]
                [:kcu1.table_name :from_table]
                [:kcu1.column_name :from_column]
                [:kcu2.table_schema :to_schema]
                [:kcu2.table_name :to_table]
                [:kcu2.column_name :to_column])
      (h/from [:information_schema.referential_constraints :rc])
      (h/join [:information_schema.key_column_usage :kcu1]
              [:and [:= :rc.constraint_catalog :kcu1.constraint_catalog]
               [:= :rc.constraint_schema :kcu1.constraint_schema]
               [:= :rc.constraint_name :kcu1.constraint_name]])
      (h/join [:information_schema.key_column_usage :kcu2]
              [:and [:= :rc.unique_constraint_catalog :kcu2.constraint_catalog]
               [:= :rc.unique_constraint_schema :kcu2.constraint_schema]
               [:= :rc.unique_constraint_name :kcu2.constraint_name]])
   ;; (db/fmt-sql)
      (db/->execute db)))

(defn get-all-cols [db]
  (->> (-> (h/select :table_schema :table_name :column_name)
           (h/from :information_schema.columns)
           (h/where :not-in :table_schema '("pg_catalog" "information_schema"))
           (db/->execute db))
       (map (fn [{:keys [table_schema table_name column_name]}]
              {:table (str table_schema "." table_name)
               :column column_name}))))

(defn build-deps [cols]
  (reduce (fn [acc {:keys [from_schema from_table from_column to_schema to_table to_column]}]
            (update acc
                    (u/vec-kw from_schema from_table)
                    assoc
                    (keyword from_column)
                    (u/vec-kw to_schema to_table to_column)))
          {}
          cols))

(comment
  (get-all-cols u/yardstick-db)
  (get-referenced-cols u/yardstick-db)
  (build-deps cols)
  ;
  )
