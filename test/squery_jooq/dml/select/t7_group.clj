(ns squery-jooq.dml.select.t7-group
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)))

(connect (slurp "/home/white/IdeaProjects/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

(print-results (q :book))

;;create.select(BOOK.AUTHOR_ID, count())
;.from(BOOK)
;.groupBy(BOOK.AUTHOR_ID)
;.fetch();

(print-results (q :book
                  (group :author_id)
                  [:author_id (count-acc)]))

;;create.selectCount()
;.from(BOOK)
;.groupBy()
;.fetch();

(print-results (q :book
                  (group)
                  [(count-acc)]))