(ns squery-jooq.c4-11column-expressions.t1-table-columns
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q  pq s ps]]
            [squery-jooq.commands.update :refer [insert uq dq]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect "postgres")

;;they can be generated as classes from JOOQ
;;or i can use case sensitive strings like
;;   Field<String> firstName = field(name("AUTHOR", "FIRST_NAME"), INTEGER);