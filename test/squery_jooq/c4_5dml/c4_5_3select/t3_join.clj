(ns squery-jooq.c4-5dml.c4-5-3select.t3-join
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

(pq :book
    (join :author (= :book.author_id :author.id))           ;;join on condition
    ;(join :author :id)  //using(id) using field
    ((= :book.published_in 1948))
    [:book.title :author.first_name :author.last_name])

;;TODO test the rest join stages
