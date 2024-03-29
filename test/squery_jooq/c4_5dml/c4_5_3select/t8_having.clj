(ns squery-jooq.dml.select.t8-having
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

;;no having, just normal filters after group => auto-become having
;;i cant have having without group like in sql (empty group is not assumed)

(pq :book)

(pq :book
   (group :author_id)
   ((= (count-acc) 2))
   [:author_id (count-acc)])

;;in squery to distinguish the having a group is needed
(pq :book
   (group)
   ((>= (count-acc) 4))
   [(count-acc)])

