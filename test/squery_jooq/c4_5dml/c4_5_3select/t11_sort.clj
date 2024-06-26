(ns squery-jooq.c4-5dml.c4-5-3select.t11-sort
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

(pq :book)

;;asc and desc with !
(pq :book
    (sort :author_id :!title)
    [:author_id :title])

;;nullsLast() with ! in the end
(pq :author
    (sort :last_name :first_name!)
    [:first_name :last_name])

;;by field index (doesnt work dont know why)
#_(pq :book
    (sort :author_id (desc (inline 2)))
    [:author_id :title])


;;TODO sort using case_ after i add that operator page 118




