(ns squery-jooq.state
  (:import (java.sql DriverManager)
           (org.jooq.conf Settings)
           (org.jooq.impl DSL)
           (org.jooq SQLDialect)))

(def ctx (atom nil))

;;map with url,username,password
(defn connect-mysql [connection-map]
  (let [connection-map connection-map
        connection-map (read-string connection-map)
        connection (DriverManager/getConnection (get connection-map "url")
                                                (get connection-map "username")
                                                (get connection-map "password"))]
    (reset! ctx (DSL/using connection SQLDialect/MYSQL))))

;;jdbc:postgresql://localhost/mydbname?user=myuser&password=mypass
(defn connect-postgres [connection-string]
  (let [connection (DriverManager/getConnection connection-string)]
    (reset! ctx (DSL/using connection SQLDialect/POSTGRES (-> (Settings.) (.withRenderFormatted true))))))