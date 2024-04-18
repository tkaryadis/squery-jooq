(ns squery-jooq.c4-5dml.c4-5-3select.t12-limit-skip-t13-withties
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q pq s ps]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect "postgres")

(pq [[:t :a] [1] [2] [2] [3] [4] [5]]
    (limit 1))

(pq [[:t :a] [1] [2] [2] [3] [4] [5]]
    (skip 1))

(pq [[:t :a] [1] [2] [2] [3] [4] [5]]
    (skip 1)
    (limit 2))

;;keeps 1,2,2,2 with-ties after sort,limit makes keep all equal
;;its limit 2 or more ONLY the equal based on sort
(pq [[:t :a] [1] [2] [2] [2] [3] [4] [5]]
    (sort :a)
    (limit 2)
    (with-ties))