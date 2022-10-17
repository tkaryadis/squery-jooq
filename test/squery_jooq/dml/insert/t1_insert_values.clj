(ns squery-jooq.dml.insert.t1-insert-values
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q insert]]
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

;;create.insertInto(AUTHOR,
;AUTHOR.ID, AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME)
;.values(100, "Hermann", "Hesse")
;.values(101, "Alfred", "Döblin")
;.execute()

(insert :book
        [:id :author_id :title :published_in :language_id]
        [[20  2 "Hello SQL1" 1999 2]
         [21  2 "Hello SQL2" 1999 2]])