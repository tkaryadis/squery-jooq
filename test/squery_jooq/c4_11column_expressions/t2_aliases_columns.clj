(ns squery-jooq.c4-11column-expressions.t2-aliases-columns
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q  pq s ps]]
            [squery-jooq.commands.update :refer [insert uq dq]]
            [squery-jooq.state :refer [connect-postgres ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

;;column.as(alias) => {:alias :column}

(pq [[:t :a :b] [1 2] [3 4]]
    [{:c :a}])

(print-sql (q [[:t :a :b] [1 2] [3 4]] [{:c :a}]))

;;unnamed columns, after operations,
;;take the name of the last function, here for example =>  sub

(pq [[:t :a :b] [1 2] [3 4]]
    [(- (+ :a 1) 2)])