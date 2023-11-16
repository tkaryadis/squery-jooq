(ns squery-jooq.c4-11column-expressions.t4-11-21accumulators.t1-t6accumulator-options
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q pq s ps]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import
    (java.sql Timestamp)
    (java.time Instant)
    (java.util Date)
    (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL)
    (org.jooq.conf Settings StatementType)))

(connect "mysql")
;(connect "postgres")

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

;;-------------------------GROUP+ACTIONS to group(accumulators-options)-------------------------


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

(pq :book
    [
     (count-acc :author_id)
     (count-distinct :author_id)
     ;(str-each :title ",")             ;;mysql error, postgres ok
     ;(str-each-distinct :author_id)
     ])


;;---------------filter----------------------------------------

(pq :book
    [(count-acc)
     (filter-acc (like "A%" :title) (count-acc))
     (filter-acc (like "%A%" :title) (count-acc))])


(pq :book
    (group :author_id)
    [:author_id
     (count-acc)
     (filter-acc (like "A%" :title) (count-acc))
     (filter-acc (like "%A%" :title) (count-acc))])

;;-----------------------------Sort------------------------------------

(pq :book
    [(conj-each :id)
     (sort-acc [:!id] (conj-each :id))])

;;----------------------------Sort within Group------------------------


;;ordered set aggregate functions
;;1)Hypothetical set functions: Functions that check for the position of a hypothetical value inside of
;  an ordered set. These include RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST.
;2)Inverse distribution functions: Functions calculating a percentile over an ordered set, including
;  PERCENTILE_CONT, PERCENTILE_DISC, or MODE.
;3)LISTAGG, which is inconsistently using the WITHIN GROUP

(pq :book
    [(sort-group [:!id] (DSL/percentileCont 0.5))])