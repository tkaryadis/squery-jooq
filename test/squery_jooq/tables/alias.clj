(ns squery-jooq.tables.alias
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q insert uq dq pq s]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql print-json-results]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect (slurp "/home/white/IdeaProjects/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

;;Author a = AUTHOR.as("a");
;Book b = BOOK.as("b");
;// Use aliased tables in your statement
;create.select()
;.from(a)
;.join(b).on(a.ID.eq(b.AUTHOR_ID))
;.where(a.YEAR_OF_BIRTH.gt(1920)
;.and(a.FIRST_NAME.eq("Paulo")))
;.orderBy(b.TITLE)
;.fetch();

(print-results
  (q {:a :author}
     (join {:b :book}
           (= :a.id :b.author_id))
     ((> :a.year_of_birth 1920) (= :a.first_name "Paulo"))
     (sort :b.title)))