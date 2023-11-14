(ns squery-jooq.columns.accumulators.accumulators
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

;;1)Types of groups
;
;  1)groupby  => accumulator on group
;  2)no-group => accumuator on table
;  3)window   => accumulator on window
;
;2)all aggregate functions can be used as window functions
;
;3)4 options (on the accumulator result)
;;   distinct
;;      few aggregate functions, i have seperate function on each one
;;   filter
;;      all aggregate functions + window functions => seperate function impl
;;   sort
;;      result will have an order  (sort the result)
;;   sort within group
;;      order will be used to get 1 single un-ordered result (pre-order to apply the calculation)
;;      aggr functions that can use it RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST ..
;;
;4) By default, SQL aggregate functions always exclude NULL values
;;  see nullOnNull

(pq :book)


;;-----------------GROUP----------------------------------------
;;create.select(BOOK.AUTHOR_ID, count())
;.from(BOOK)
;.groupBy(BOOK.AUTHOR_ID).fetch();

(pq :book
    (group :author_id)
    [:author_id (count-acc)])

;;--------------NO-GROUP---------------------------------------
;;create.select(count(), sum(BOOK.ID))
;.from(BOOK).fetch();

(pq :book
    [(count-acc) (sum :id)])

;;create.select(count().plus(sum(BOOK.ID)).plus(1))
;.from(BOOK).fetch();

(pq :book
    [(+ (count-acc) (sum :id) 1)])

;;-------------------Distinct---------------------------------


;;create.select(
;count(BOOK.AUTHOR_ID),
;countDistinct(BOOK.AUTHOR_ID),
;groupConcat(BOOK.AUTHOR_ID),
;groupConcatDistinct(BOOK.AUTHOR_ID))
;.from(BOOK).fetch();

(print-results
  (q :book
     [(count-acc :author_id)
      (count-distinct :author_id)
      (str-each :author_id)
      (str-each-distinct :author_id)]))


;;---------------filter----------------------------------------

;;create.select(
;count(),
;count().filterWhere(BOOK.TITLE.like("A%")),
;count().filterWhere(BOOK.TITLE.like("%A%")))
;.from(BOOK)

(print-results
  (q :book
     [(count-acc)
      (filter-acc (like "A%" :title) (count-acc))
      (filter-acc (like "%A%" :title) (count-acc))]))

;;create.select(
;BOOK.AUTHOR_ID,
;count(),
;count().filterWhere(BOOK.TITLE.like("A%")),
;count().filterWhere(BOOK.TITLE.like("%A%")))
;.from(BOOK)
;.groupBy(BOOK.AUTHOR_ID)

(print-results
  (q :book
     (group :author_id)
     [:author_id
      (count-acc)
      (filter-acc (like "A%" :title) (count-acc))
      (filter-acc (like "%A%" :title) (count-acc))]))

;;-----------------------------Sort------------------------------------

;;create.select(
;arrayAgg(BOOK.ID),
;arrayAgg(BOOK.ID).orderBy(BOOK.ID.desc()))
;.from(BOOK)

(print-results
  (q :book
     [(conj-each :id)
      (sort-acc [:!id] (conj-each :id))]))

;;----------------------------Sort within Group------------------------

;;create.select(
;percentileCont(0.5).withinGroupOrderBy(BOOK.ID))
;.from(BOOK)

(print-results
  (q :book
     [(sort-group [:!id] (DSL/percentileCont 0.5))]))


;;----------------------------------------------------------------------
;;create.select(
;boolAnd(BOOK.ID.lt(4)),
;boolAnd(BOOK.ID.lt(5)))
;.from(BOOK)

(print-results (q :book
                  [(and-acc (< :id 4))
                   (and-acc (< :id 5))]))