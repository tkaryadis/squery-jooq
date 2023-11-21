(ns squery-jooq.c4-11column-expressions.t4-11-21accumulators.t1-t6group-and-options
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

;(connect "mysql")
(connect "postgres")

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

;;group here is optional but i added it to be clear
(pq :book
    (group)
    [(count-acc) (sum-acc :id)])

;;operators in accumulator results
(pq :book
    (group)
    [(+ (count-acc) (sum-acc :id) 1)])

(pq :book
    (group :author_id)
    [:author_id (count-acc)])

;;----------------------options are applied first on the group, and then the accumulator------------------

;;TODO make options more squery like

;;-------------------Distinct---------------------------------

(pq :book
    (group)
    [
     (count-acc :author_id)
     (count-distinct :author_id)
     (str-each :title ",")
     (str-each-distinct :author_id)
     ])

;;---------------filter----------------------------------------

;;filter on group and then count

(pq :book
    (group)
    [(count-acc)
     ;;A any-chars  (just start with A)
     (filter-acc (like "A%" :title) (count-acc))
     ;;any-chars A any-chars (A in any place)
     (filter-acc (like "%A%" :title) (count-acc))])


(pq :book
    (group :author_id)
    [:author_id
     (count-acc)
     (filter-acc (like "A%" :title) (count-acc))
     (filter-acc (like "%A%" :title) (count-acc))])

;;-----------------------------Sort------------------------------------

;;sort each group and then conj
;;here i order the accumulator result

(pq [[:t :id :a :b] [1 1 1] [2 1 2] [3 1 0] [4 2 1]]
    (group :a)
    [(aconj-each :id)
     (sort-acc [:!id] (aconj-each :id))])

;;----------------------------Sort within Group-----------------------

;;here i use order, to calculate a single value (above i kept all the ordered group)
;;its like ordering the group to use it, to calculate that single value

;;one example is rank, where i need to order the group to find the rank
;;   for each row inside the group

;;ordered set aggregate functions
;;1)RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST.
;;2)PERCENTILE_CONT, PERCENTILE_DISC, or MODE.
;;3)LISTAGG (aggregates to a string)

(pq :book
    [(sort-group [:!id] (DSL/percentileCont 0.5))])