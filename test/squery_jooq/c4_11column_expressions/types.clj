(ns squery-jooq.c4-11column-expressions.types
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.query :refer [q pq s ps]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import
    (java.sql Timestamp)
    (java.time Instant)
    (java.util Date)
    (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL)
    (org.jooq.conf Settings StatementType)))

;(connect "mysql")
(connect "postgres")

;;sql array to json-array (get needs jsonb array)
(ps [(get (into [] (sql-array 1 5)) 1)])

(ps [(string? 1)])