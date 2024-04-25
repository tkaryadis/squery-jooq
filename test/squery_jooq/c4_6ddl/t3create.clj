(ns squery-jooq.c4-6ddl.t3create
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer :all]
            [squery-jooq.commands.update :refer :all]
            [squery-jooq.commands.admin :refer :all]
            [squery-jooq.state :refer [connect-postgres ctx]]
            [squery-jooq.schema :refer [schema-types]]
            [squery-jooq.printing :refer [print-results print-sql ]]
            [clojure.core :as c])
  (:refer-clojure)
  (:import
    (java.sql Timestamp)
    (java.time Instant)
    (java.util Date)
    (org.jooq Constraint ConstraintTypeStep CreateTableElementListStep DataType SQLDialect DSLContext Field Select Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL QOM$Lateral SQLDataType SelectImpl)
    (org.jooq.conf Settings StatementType)))

;;TODO SKIPPED (some are payed only jooq)
;;4.6.3.2. CREATE DOMAIN
;;4.6.3.3. CREATE FUNCTION
;;4.6.3.5. CREATE PROCEDURE
;;4.6.3.7. CREATE SEQUENCE
;;4.6.3.9. CREATE TRIGGER
;;4.6.3.10. CREATE TYPE
;;4.6.3.11. CREATE VIEW

(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

;;create db
;(create-database "mydb")

#_(create-indexes :author
                (index [:first_name :!last_name] {:name "myindex1"})
                (index [:!first_name] {:name "myindex2" :unique true}))

;;TODO covering + partial indexes page 176

;;Schema
;;when i create a database postgres auto-creates schema named 'public'
;;schema is a logical container within a database that holds a collection of related database objects,
;;such as tables, views, and indexes. It provides a way to organize and manage objects within a database.

;(create-schema "new-schema")

;;Create table
#_(create-table :mytable1
              [:col1
               [:col2 :integer false]
               [:col3 :long true]]
              [(DSL/primaryKey (into-array String ["col1"]))])


;;Create table auto-increment primary key id
#_(create-table :temptable
              [[:id (-> (SQLDataType/INTEGER)
                        (.nullable false)
                        (.identity true))]
               :name]
              [(DSL/primaryKey (into-array String ["id"]))])

;;Create table(from select)
;(create-table :book-copy (q :book))
