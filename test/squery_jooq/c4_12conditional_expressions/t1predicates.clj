(ns squery-jooq.c4-12conditional-expressions.t1predicates
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q pq s ss ps]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import
    (java.sql Timestamp)
    (java.time Instant)
    (java.util Date)
    (org.jooq SQLDialect DSLContext Field Select Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL QOM$Lateral SelectImpl)
    (org.jooq.conf Settings StatementType)))

;(connect "mysql")
(connect "postgres")

;;SQL types
;;  1 or TRUE
;;  0 or FALSE
;;  NULL or UNKNOWN

;;NULL comparitions
;; [ANY] = NULL yields NULL (not FALSE)
;; [ANY] != NULL yields NULL (not TRUE)
;; NULL = NULL yields NULL (not TRUE)
;; NULL != NULL yields NULL (not FALSE)

;;JOOQ has a clause for them = Condition
;; for example (= :a 1) produce Condition instances

;;case
#_(pq [[:t :a] [0] [1] [2] [3]]
    [:a
     (cond (= :a 0) 5
           (= :a 1) 20
           :else -10)])

;;logical
#_(pq [[:t :a :b] [true false] [true true] [false false]]
    [(not :a)
     (and :a :b)
     (and :a :b (and :a :a))
     (or :a :b)
     (xor :a :b)
     (or (xor :a :b) (and :a :b))
     ])

;;comparison
(pq [[:t :a] [1]]
    [(= :a 1)
     (and (= :a 1) (= :a 1))
     (not= :a 1)
     (> :a 1)
     (< :a 1)
     (>= :a 1)
     (<= :a 1)
     ;; 2<=a<=3
     (<< :a 2 3)
     ;; 1<=a<=3 , order i give the 3,1 doesnt matter
     (<> :a 3 1)])

;;quantifier any doesn work on postgres?




