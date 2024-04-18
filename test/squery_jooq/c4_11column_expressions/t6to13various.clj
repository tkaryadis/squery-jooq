(ns squery-jooq.c4-11column-expressions.t6to13various
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q  pq s ps]]
            [squery-jooq.commands.update :refer [insert uq dq]]
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

(connect "postgres")

;;Collations => define the sort order of datatypes like varchar

;;doesnt work on postgres, but its like that the call
#_(pq :book
    (sort (collate :title "utf8_bin")))

;;Arithmetic expressions i implemented many
(pq [[:t :a :b] [1 2]]
    [(div (* (cast :a :double) 2) 5)
     (pow 2 3)])

;;numeric functions
;;TODO has many others like
;; ACOS,ASIN,ATAN,ATAN2,COS,COSH,COT,COTH,DEG,RAD,SIN,SINH,TAN
;; SIGN    ;;useless
(ps [(abs -5)
     (ceil 1.2)
     (floor 1.8)
     e
     (exp 1)
     (max 1 2 10 3)
     (ln 1)
     ;(rand)
     (round 1.5)
     (sqrt 4)
     (sign -10)])

;;Datetime arithmetic expressions

;;add works with dates adding DAYS
(ps [(+ (Timestamp/from (Instant/now)) 3)])

;see also DSL's timestampDiff() and dateDiff() functions, as well
;INTERVAL YEAR TO MONTH: org.jooq.types.YearToMonth
;INTERVAL DAY TO SECOND: org.jooq.types.DayToSecond

(ps [(str "a" (lower-case "B") (upper-case "c"))])

;;just pick from vec based on index, useless dont use
(pq [[:t :a :b] [1 2]]
    [(choose 0 [1 2 3])
     (choose 1 [:a :b])])

;;picks the fist not nil arg, arg can be column
(ps [(coalesce nil nil 10)])

;;decode skipped useless

;;if
(ps [(if- (= 1 1) 2 3)])

;;NULLIF skipped its just (if (= arg1 arg2) nil avalue)
;;NVL skipped also
;;NVL2 skipped also

;;Bitwise functions skipped

;;TODO add String functions+datetime functions