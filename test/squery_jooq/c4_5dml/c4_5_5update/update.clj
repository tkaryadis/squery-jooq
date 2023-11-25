(ns squery-jooq.dml.update.update
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.query :refer [q insert uq]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql print-json-results]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect (slurp "/home/white/IdeaProjects/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

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











