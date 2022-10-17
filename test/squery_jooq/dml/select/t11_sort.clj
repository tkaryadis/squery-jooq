(ns squery-jooq.dml.select.t11-sort
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

;;create.select(BOOK.AUTHOR_ID, BOOK.TITLE)
;.from(BOOK)
;.orderBy(BOOK.AUTHOR_ID.asc(), BOOK.TITLE.desc())
;.fetch();

(print-results (q :book
                  (sort :author_id :!title)
                  [:author_id :title]))

;;create.select(BOOK.AUTHOR_ID, BOOK.TITLE)
;.from(BOOK)
;.orderBy(one().asc(), inline(2).desc())
;.fetch();

(print-results (q :book
                  (sort :author_id (desc (inline 2)))
                  [:author_id :title]))

;;create.select(
;AUTHOR.FIRST_NAME,
;AUTHOR.LAST_NAME)
;.from(AUTHOR)
;.orderBy(AUTHOR.LAST_NAME.asc(),
;AUTHOR.FIRST_NAME.asc().nullsLast())
;.fetch();

(print-results (q :author
                  (sort :last_name :first_name!)
                  [:first_name :last_name]))

;;TODO
;;Ordering using CASE expressions and some more, after i add the case operator



