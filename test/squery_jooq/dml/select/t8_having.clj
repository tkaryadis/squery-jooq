(ns squery-jooq.dml.select.t8-having
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
;.groupBy(AUTHOR_ID)
;.having(count().ge(2))
;.fetch();

(print-results (q :book
                  (group :author_id)
                  ((>= (count-acc) 2))
                  [:author_id (count-acc)]))

;;create.select(count(*))
;.from(BOOK)
;.having(count().ge(4))
;.fetch();

;;in squery to distinguish the having a group is needed
(print-results (q :book
                  (group)
                  ((>= (count-acc) 4))
                  [(count-acc **)]))

