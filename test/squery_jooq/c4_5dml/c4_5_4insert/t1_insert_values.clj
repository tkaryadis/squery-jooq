(ns squery-jooq.c4_5dml.c4_5_4insert.t1-insert-values
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q pq]]
            [squery-jooq.commands.update :refer [insert]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect "postgres")

(pq :book)

(insert :book
        [:id :author_id :title :published_in :language_id]
        [[20  2 "Hello SQL1" 1999 2]
         [21  2 "Hello SQL2" 1999 2]])