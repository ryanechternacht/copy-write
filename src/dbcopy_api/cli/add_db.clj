(ns dbcopy-api.cli.add-db
  (:require [clojure.string :as str]))

(defn- get-input
  ([prompt] (get-input prompt {}))
  ([prompt {:keys [required] :as opts}]
   (let [text (cond-> prompt
                required (str " (required)")
                :always (str ": "))]
     (print text)
     (flush)
     (let [value (str/trim (read-line))]
       (cond
         (and (= "" value) required) (get-input prompt opts)
         :else value)))))

(defn- add-postgres []
  (let [nickname (get-input "Nickname" {:required true})
        host (get-input "Host" {:required true})
        user (get-input "User" {:required true})
        password (get-input "Password")
        dbname (get-input "Db Name" {:required true})]
    (spit (str ".dbcopy/dbs/" nickname ".edn") {:dbtype "postgresql"
                                                :nickname nickname
                                                :host host
                                                :user user
                                                :password password
                                                :dbname dbname})))

(defn add-db [{:keys [postgres]}]
  (cond
    postgres (add-postgres)
    :else (do
            (println "Please choose a database. Use add-db -? for help")
            (flush))))