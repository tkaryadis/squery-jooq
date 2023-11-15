(ns squery-jooq.state
  (:import (java.sql DriverManager)
           (org.jooq.impl DSL)
           (org.jooq SQLDialect)))

(def ctx (atom nil))

;; String userName = "root";
;        String password = "";
;        String url = "jdbc:mysql://localhost:3306/library";
;
;        // Connection is the only JDBC resource that we need
;        // PreparedStatement and ResultSet are handled by jOOQ, internally
;        try (Connection conn = DriverManager.getConnection(url, userName, password)) {



;;init the ctx context that will auto-used from queries
#_(defn connect
  ([connection-str dialect]
   (let [connection (DriverManager/getConnection connection-str)]
     (reset! ctx (DSL/using connection SQLDialect/POSTGRES))))
  ([connection-str dialect settings]
   (let [connection (DriverManager/getConnection connection-str)]
     (reset! ctx (DSL/using connection SQLDialect/POSTGRES settings)))))

;;expect a string that is a clojure map, with url,username,password
(defn connect [dialect]
  (let [connection-map (slurp "/home/white/IdeaProjects/squery/squery-jooq/connection-map")
        connection-map (read-string connection-map)
        connection (DriverManager/getConnection (get connection-map "url")
                                                (get connection-map "username")
                                                (get connection-map "password"))]
    (reset! ctx (DSL/using connection dialect))))