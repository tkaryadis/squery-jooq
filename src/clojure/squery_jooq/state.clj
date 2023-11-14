(ns squery-jooq.state
  (:import (java.sql DriverManager)
           (org.jooq.impl DSL)
           (org.jooq SQLDialect)))

(def ctx (atom nil))

;;init the ctx context that will auto-used from queries
(defn connect
  ([connection-str dialect]
   (let [connection (DriverManager/getConnection connection-str)]
     (reset! ctx (DSL/using connection SQLDialect/POSTGRES))))
  ([connection-str dialect settings]
   (let [connection (DriverManager/getConnection connection-str)]
     (reset! ctx (DSL/using connection SQLDialect/POSTGRES settings)))))
