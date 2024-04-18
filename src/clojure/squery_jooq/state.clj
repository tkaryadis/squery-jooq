(ns squery-jooq.state
  (:import (java.sql DriverManager)
           (org.jooq.conf Settings)
           (org.jooq.impl DSL)
           (org.jooq SQLDialect)))

(def ctx (atom nil))

(def connection-map (try (read-string  (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-map"))
                         (catch Exception e {})))

(def connection-string (try (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string")
                            (catch Exception e "")))

;;postgress setting
;;(-> (Settings.) (.withRenderFormatted true))

;;expect a string that is a clojure map, with url,username,password
(defn connect [db-name]
  (cond

    (= db-name "mysql")
    (let [connection-map connection-map
          connection-map (read-string connection-map)
          connection (DriverManager/getConnection (get connection-map "url")
                                                  (get connection-map "username")
                                                  (get connection-map "password"))]
      (reset! ctx (DSL/using connection SQLDialect/MYSQL)))

    :else
    (let [_ (prn "xxx" connection-string)
          connection (DriverManager/getConnection connection-string)]
      (reset! ctx (DSL/using connection SQLDialect/POSTGRES (-> (Settings.) (.withRenderFormatted true)))))))