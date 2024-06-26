(ns squery-jooq.c4-5dml.c4-5-5update.update
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q]]
            [squery-jooq.commands.update :refer [insert uq]]
            [squery-jooq.state :refer [connect-postgres ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

(print-results (q :book))

(uq :book
    {:title "aTitle"})

(print-results (q :book))

(uq :book
    {:title (q :book
               (limit 1)
               ["aNewTtitle"])})

(print-results (q :book))

;;create.update(AUTHOR)
;.set(row(AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME),
;select(PERSON.FIRST_NAME, PERSON.LAST_NAME)
;.from(PERSON)
;.where(PERSON.ID.eq(AUTHOR.ID))
;)
;.where(AUTHOR.ID.eq(3))
;.execute();

(print-results (q :author))

(uq :author
    {(row :first_name :last_name) (q :author
                                     (limit 1)
                                     ["aNewTfirst1" "aNEwlast1"])})

(print-results (q :author))


;;TODO has more











