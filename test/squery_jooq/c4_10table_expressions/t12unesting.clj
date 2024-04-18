(ns squery-jooq.c4-10table-expressions.t12unesting
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q sq pq s ss ps]]
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

;;explode array column
(ps [(unwind-array (array 1 2 3))])

(pq (unwind-array-to-table (array 1 2 3 4) [:t :a]))
;;repeat the same values in 2 columns, max is 2 columns
(pq (unwind-array-to-table (array 1 2 3 4) [:t :a :b]))

;;There is for JSON also
(ps [(unwind [1 2 3])])
(pq (unwind-to-table [1 2 3 4] [:t :a]))

;;TODO 4.10.13-17 not important