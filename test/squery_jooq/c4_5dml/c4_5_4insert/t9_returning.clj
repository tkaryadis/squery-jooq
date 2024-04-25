(ns squery-jooq.c4-5dml.c4-5-4insert.t9-returning
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q pq]]
            [squery-jooq.commands.update :refer [insert dq]]
            [squery-jooq.commands.admin :refer [create-table drop-table-if-exists]]
            [squery-jooq.state :refer [connect-postgres ctx]]
            [squery-jooq.printing :refer [print-results print-sql]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL SQLDataType)
           (org.jooq.conf Settings StatementType)))

(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

;(pq :book)

;;dslContext.createTable(EXAMPLE_TABLE)
;                  .column(EXAMPLE_TABLE.ID, SQLDataType.INTEGER.nullable(false).identity(true))
;                  .column(EXAMPLE_TABLE.NAME, SQLDataType.VARCHAR(255).nullable(false))
;                  .constraints(DSL.constraint().primaryKey(EXAMPLE_TABLE.ID))
;                  .execute();

(drop-table-if-exists :temptable)

(create-table :temptable
              [[:id (-> (SQLDataType/INTEGER)
                         (.nullable false)
                         (.identity true))]
               :name]
              [(DSL/primaryKey (into-array String ["id"]))])

(insert :temptable
        [:name]
        [["kostas"]
         ["mpampis"]
         ["mixalis"]])

(dq :temptable
    ((= :name "mpampis")))

(prn (insert :temptable
             [:name]
             [["takis"]
              ["eleni"]]
             [:id :name]))

(prn (insert :temptable
             [:name]
             [["john"]]
             [:id]))