(ns squery-jooq.c4-5dml.c4-5-3select.t7-group
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q pq s ps]]
            [squery-jooq.state :refer [connect-postgres ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

;;accumulators are using only in select

(pq :book)

;;group by field, each distinct value its own group
(pq :book
    (group :author_id)
    [:author_id (count-acc)])

;;all table 1 group
(pq :book
    (group)
    [(count-acc)])

(prn (type (q :book)))

;;TODO group by tables page 112
;;org.jooq.Table expression extends the org.jooq.GroupField

;;TODO group+rollup+cube+grouping sets




