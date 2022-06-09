(ns dbcopy-api.utils)

(defn vec-kw [& xs]
  (vec (map keyword xs)))

(defn make-table-kw [[schema table]]
 (keyword (str (name schema) "." (name table))))

(def yardstick-db {:dbtype "postgresql"
                   :dbname "yardstick"
                   :host "127.0.0.1"
                   :user "ryan"
                   :password nil
                   :ssl false})
