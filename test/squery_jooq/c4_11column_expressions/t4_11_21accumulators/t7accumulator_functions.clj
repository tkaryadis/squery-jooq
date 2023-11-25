(ns squery-jooq.c4-11column-expressions.t4-11-21accumulators.t7accumulator-functions
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.query :refer [q pq s ps]]
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

;;aggregate functions ignores NULL values when they produce 1 value
;;but not when i collect to arrays(json or normal ones)

(pq [[:t :a] [1] [2] [3]]
    (group)
    [(rand-acc :a)  ;;pick any random value from the group
     (count-acc)])

(pq [[:t :a] [1] [2] [3]]
    (group)
    [(avg-acc :a)])

(pq [[:t :a] [1] [2] [3]]
    (group)
    [(and-acc (< :a 4))    ;;true if true for all rows
     (or-acc (> :a 2))])   ;;true if at least for 1 row

;;hypothetical set functions

;;finds the rank of a=3 (lit arg= the value that i want to find its rank)
(pq [[:t :a] [1] [1] [2] [2] [3] [4]]
    (group)
    [(sort-group [:a] (rank (lit 3)))
     (sort-group [:a] (dense-rank (lit 3)))])


;;collect to array, optionally sort also
(pq [[:t :id :a :b] [1 1 1] [2 1 2] [3 1 0] [4 2 1]]
    (group :a)
    [(aconj-each :id)
     (sort-acc [:!id] (aconj-each :id))
     ;;json_array
     (conj-each :a)])

;;arrays keep the nulls, but i can ignore them like bellow
(pq [[:t :a] [1] [2] [nil] [4]]
    [(aconj-each :a)
     ;;the default
     (keep-nil (conj-each :a))
     (ignore-nil (conj-each :a))])

;;needs 2 args,key col, value col, collect to map
;;if same key many times, keeps the last (if not ordered, non determenistic)
;;key nil => always error
;;value can be nil, and i can ignore it
(pq [[:t :a :b] [1 2] [3 nil] [5 6]]
    [(merge-acc :a :b)
     (ignore-nil (merge-acc :a :b))])

;;aggregate to string
(pq [[:t :a :b] [1 2] [3 nil] [5 6]]
    [(str-each :a)
     (sort-acc [:!a] (str-each :a))])

(pq [[:t :a :b] [1 2] [3 nil] [5 6]]
    [(max-acc :a)
     (min-acc :a)])

;;collect to multi-set, its like array or groups (not only pairs, can be many)
(pq [[:t :a :b ] [1 2 3] [1 3 4] [2 5 6]]
    [(multiset-acc :a :b)
     (type (multiset-acc :a :b))])