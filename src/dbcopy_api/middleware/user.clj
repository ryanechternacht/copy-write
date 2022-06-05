(ns dbcopy-api.middleware.user
  (:require [yardstick-api.data.users :as d-users]))

;; (defn- wrap-user-impl [handler {:keys [session db] :as request}]
;;   (if (:user-id session)
;;     (let [user (d-users/get-user-by-id db (:user-id session))]
;;       (handler (assoc request :user user)))
;;     (handler request)))

; This form has the advantage that changes to wrap-debug-impl are
; automatically reflected in the handler (due to the lookup in `wrap-user`)
;; (defn wrap-user [h] (partial #'wrap-user-impl h))
