(ns squery-jooq.c4-5dml.c4-5-3select.t0-with
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


(pq (with :a (s [{:x 1} {:y 2}]))
    :a)