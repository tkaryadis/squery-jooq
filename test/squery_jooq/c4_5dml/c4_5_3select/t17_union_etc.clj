(ns squery-jooq.dml.select.t17-union-etc
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.query :refer [q]]
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

;;create.selectFrom(BOOK).where(BOOK.ID.eq(3))
;.unionAll(
;create.selectFrom(BOOK).where(BOOK.ID.eq(2)))
;.fetch();

(print-results (q :book
                  ((= :id 3))
                  (union-all (q :book
                                ((= :id 2))))))

;;create.selectFrom(AUTHOR)
;.orderBy(AUTHOR.DATE_OF_BIRTH.asc()).limit(1)
;.union(
;selectFrom(AUTHOR)
;.orderBy(AUTHOR.DATE_OF_BIRTH.desc()).limit(1))
;.orderBy(1)
;.fetch();

(print-results (q :author
                  (sort :date_of_birth)
                  (limit 1)
                  (union-all (q :author
                                (sort :!date_of_birth)
                                (limit 1)))
                  (sort (inline 1))))