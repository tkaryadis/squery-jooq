(ns squery-jooq.c4-5dml.c4-5-6delete.delete
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q]]
            [squery-jooq.commands.update :refer [insert uq dq]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect "postgres")

(print-results (q :book))

(dq :book
    ((= :id 1)))

(print-results (q :book))