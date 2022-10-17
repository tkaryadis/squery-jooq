(ns squery-jooq.columns.conditionals
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q insert uq dq pq]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql print-json-results]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect (slurp "/home/white/IdeaProjects/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

(pq :book)

(pq :book
    [(if- (> :published_in 1988) 1 2)])

(pq :author)

;;when(AUTHOR.FIRST_NAME.eq("Paulo"), "brazilian")
;.when(AUTHOR.FIRST_NAME.eq("George"), "english")
;.otherwise("unknown");

(pq :author
    [:first_name {:ethnicity (cond (= :first_name "Paulo") "brazilian"
                                   (= :first_name "George") "english"
                                   :else "unknown")}])

(pq :author
    [:first_name {:ethnicity (cond true "brazilian"
                                   (= :first_name "George") "english"
                                  :else "unknown")}])




