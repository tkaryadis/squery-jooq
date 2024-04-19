(ns squery-jooq.c4-11column-expressions.types
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q  pq s ps]]
            [squery-jooq.commands.update :refer [insert uq dq]]
            [squery-jooq.state :refer [connect-postgres ctx]]
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
(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

;;sql array to json-array (get needs jsonb array)
(ps [(get (into [] (sql-array 1 5)) 1)])

(ps [(string? 1)])