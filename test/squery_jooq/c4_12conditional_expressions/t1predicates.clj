(ns squery-jooq.c4-12conditional-expressions.t1predicates
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q  pq s ps sq ss]]
            [squery-jooq.commands.update :refer [insert uq dq]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]]
            [clojure.core :as c])
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
(pq [[:t :a] [0] [1] [2] [3]]
    [:a
     (cond (= :a 0) 5
           (= :a 1) 20
           :else -10)])

;;logical
(pq [[:t :a :b] [true false] [true true] [false false]]
    [(not :a)
     (and :a :b)
     (and :a :b (and :a :a))
     (or :a :b)
     (xor :a :b)
     (or (xor :a :b) (and :a :b))
     ])

;;comparison
(pq [[:t :a] [1] [nil]]
    [(= :a 1)
     ;;safe nil==nil
     (=safe :a nil)
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

;;exist predicate
(ps [(exists? (sq [[:t :a] [1]] ((= :a 1))))
     (not-exists? (sq [[:t :a] [1]] ((= :a 1))))])

(pq [[:t :a :b] [1 1]]
    [(in :a 1 2 3)
     (in :a :b)
     (in :a 2 3)
     (in :a (ss [1]))])

(pq [[:t :a] ["Adssffsd"] ["adffsAafdsfd"] ["A"]]
    [;;A in the start
     (like "A%" :a)
     ;;somewhere a A, left right whatever or empty
     (like "%A%" :a)
     (like (c/str "%" "A" "%") :a)])

(ps [;;_ single character not zero
     (similar "_ta%" "ktaSDSFFFSF")
     (similar "_ta%" "taSDSFFFSF")
     (similar "ta%" "ktaSDSFFFSF")])

(pq [[:t :a] [nil] [1]]
    [(nil? :a)])



