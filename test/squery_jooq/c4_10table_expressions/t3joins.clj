(ns squery-jooq.c4-10table-expressions.t3joins
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.query :refer [q sq pq s ss ps]]
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

;;CROSS JOIN: A cross product
;;INNER JOIN: A cross product filtering on matches
;;OUTER JOIN: A cross product filtering on matches, additionally producing some unmatched rows
;;SEMI JOIN: A check for existence of rows from one table in another table (using EXISTS or IN)
;;ANTI JOIN: A check for non-existence of rows from one table in another table (using NOT EXISTS
;;  or some conditions NOT IN)

;;ON: Expressing join predicates explicitly
;ON KEY: Expressing join predicates explicitly or implicitly based on a FOREIGN KEY
;USING: Expressing join predicates implicitly based on an explicit set of shared column names in
;both tables
;NATURAL: Expressing join predicates implicitly based on an implicit set of shared column names
;in both tables

;;APPLY or LATERAL: Ordering the join tree from left to right, allowing the right side to access rows
;from the left side
;PARTITION BY on OUTER JOIN: To fill the gaps in a report that uses OUTER JOIN

;;TODO add examples