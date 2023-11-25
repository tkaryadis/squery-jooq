(ns squery-jooq.c4-6ddl.t3create
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer :all]
            [squery-jooq.commands.update :refer :all]
            [squery-jooq.commands.admin :refer :all]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]]
            [clojure.core :as c])
  (:refer-clojure)
  (:import
    (java.sql Timestamp)
    (java.time Instant)
    (java.util Date)
    (org.jooq SQLDialect DSLContext Field Select Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL QOM$Lateral SelectImpl)
    (org.jooq.conf Settings StatementType)))

;(connect "mysql")
(connect "postgres")

;;create db
;(create-database "mydb")

(create-indexes :author
                (index [:first_name :!last_name] {:name "myindex1"})
                (index [:!first_name] {:name "myindex2" :unique true}))

;;TODO covering + partial indexes page 176

;;TODO 4.6.3.5. CREATE PROCEDURE

#_(create-schema "new-schema")

;;TODO 4.6.3.7. CREATE SEQUENCE


