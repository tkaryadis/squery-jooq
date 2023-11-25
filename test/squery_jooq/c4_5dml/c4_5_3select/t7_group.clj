(ns squery-jooq.dml.select.t7-group
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.query :refer [q pq s ps]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect (slurp "/home/white/IdeaProjects/squery/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

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




