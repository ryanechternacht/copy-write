(ns dbcopy-api.utils)

(defn vec-kw [& xs]
  (vec (map keyword xs)))

(defn make-table-str [[schema table]]
  (str (name schema) "." (name table)))

(defn make-table-kw [t]
  (keyword (make-table-str t)))

(def yardstick-db {:dbtype "postgresql"
                   :dbname "yardstick"
                   :host "127.0.0.1"
                   :user "ryan"
                   :password nil
                   :ssl false})
