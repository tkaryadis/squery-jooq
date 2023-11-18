(ns squery-jooq.state
  (:import (java.sql DriverManager)
           (org.jooq.conf Settings)
           (org.jooq.impl DSL)
           (org.jooq SQLDialect)))

(def ctx (atom nil))

;;postgress setting
;;(-> (Settings.) (.withRenderFormatted true))

;;expect a string that is a clojure map, with url,username,password
(defn connect [db-name]
  (cond

    (= db-name "mysql")
    (let [connection-map (slurp "/home/white/IdeaProjects/squery/squery-jooq/connection-map")
          connection-map (read-string connection-map)
          connection (DriverManager/getConnection (get connection-map "url")
                                                  (get connection-map "username")
                                                  (get connection-map "password"))]
      (reset! ctx (DSL/using connection SQLDialect/MYSQL)))

    :else
    (let [connection-str (slurp "/home/white/IdeaProjects/squery/squery-jooq/connection-string")]
      (let [connection (DriverManager/getConnection connection-str)]
        (reset! ctx (DSL/using connection SQLDialect/POSTGRES (-> (Settings.) (.withRenderFormatted true))))))))