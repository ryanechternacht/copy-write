(ns dbcopy-api.routes
  (:require [compojure.core :refer [defroutes GET POST]]
            [ring.util.response :refer [response not-found]]))

(def GET-root-healthz
  (GET "/" [:as req]
    (println req)
    (response "I'm here")))

(defroutes routes
  #'GET-root-healthz)