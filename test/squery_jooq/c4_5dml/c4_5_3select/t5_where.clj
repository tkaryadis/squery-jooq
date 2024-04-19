(ns squery-jooq.c4-5dml.c4-5-3select.t5-where
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


;;and is implicit, i can add it but not need
(print-sql (q :book
              ((= :author_id 1) (= :title "1984"))))

;;or example
(print-sql (q :book
              ((or (= :author_id 1) (= :title "Brida")))))

