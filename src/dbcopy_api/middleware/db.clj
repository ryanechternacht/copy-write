(ns dbcopy-api.middleware.db
  (:require [dbcopy-api.ingest :as ing]))

;; TODO probaby use component or mount for this
;; TODO probably setup a connection pool for this (c3p0?)

; This form has the advantage that changes to wrap-db-impl are
; automatically reflected in the handler (due to the lookup in `wrap-db`)
(defn- wrap-db-impl [handler {{pg-db :pg-db} :config :as request}]
  (handler (assoc request :db pg-db)))

(defn wrap-db [h] (partial #'wrap-db-impl h))

(comment
  (def in-mem-db {:db (atom {:dbtype "postgresql"
                             :dbname "yardstick"
                             :host "127.0.0.1"
                             :user "ryan"
                             :password nil
                             :ssl false})
                  :deps (atom {[:public :school_assessment_instance] {:school_id [:public :school :id]},
                               [:public :student] {:school_id [:public :school :id]},
                               [:public :student_assessment]
                               {:student_id [:public :student :id], :school_assessment_instance_id [:public :school_assessment_instance :id]},
                               [:public :student_obstacle] {:student_id [:public :student :id]},
                               [:public :student_support] {:student_id [:public :student :id]},
                               [:public :student_opportunity] {:student_id [:public :student :id]},
                               [:public :assessment_star_v1] {:school_assessment_instance_id [:public :school_assessment_instance :id]},
                               [:public :assessment_map_v1] {:school_assessment_instance_id [:public :school_assessment_instance :id]}})
                  :slurped-data (atom nil)
                  :spat-rows (atom nil)})
  ; 
  )

(def in-mem-db {:db (atom nil)
                :deps (atom nil)
                :slurped-data (atom nil)
                :spat-rows (atom nil)})

(defn- wrap-in-mem-db-impl [handler request]
  (handler (assoc request :in-mem-db in-mem-db)))

(defn wrap-in-mem-db [h] (partial #'wrap-in-mem-db-impl h))
