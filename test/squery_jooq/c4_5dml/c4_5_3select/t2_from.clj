(ns squery-jooq.c4-5dml.c4-5-3select.t2-from
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

(pq :author)

(pq {:a :author}
    [:a.first_name])

(pq [[:t :a :b] [1 2] [3 4]])

(def nested (q :author :authortable))
(pq nested
    [:authortable.first_name])

;cartesian
(pq (from [:book :author]))

;;cartesian alias
(pq (from [{:b :book} {:a :author}]))
