(ns squery-jooq.dml.select.t5-where
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

;;create.select()
;.from(BOOK)
;.where(BOOK.AUTHOR_ID.eq(1))
;.and(BOOK.TITLE.eq("1984"))
;.fetch();

;;and is implicit, i can add it but not need
(print-results (q :book
                  ((= :author_id 1) (= :title "1984"))))

;;or example
(print-results (q :book
                  ((or (= :author_id 1) (= :title "Brida")))))

