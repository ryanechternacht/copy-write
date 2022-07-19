(ns dbcopy-api.utils
  (:require [clojure.java.io :as io]))

(def yardstick-db {:dbtype "postgresql"
                   :dbname "yardstick"
                   :host "127.0.0.1"
                   :user "ryan"
                   :password nil
                   :ssl false})

(defn vec-kw [& xs]
  (vec (map keyword xs)))

(defn make-table-str [[schema table]]
  (str (name schema) "." (name table)))

(defn make-table-kw [t]
  (keyword (make-table-str t)))

(defn read-files-from-folder
  "Given a folder slurps and `read-strings` every file in it"
  [folder]
  (->> (io/file folder)
       file-seq
       (filter #(.isFile %))
       (map slurp)
       (map read-string)))
