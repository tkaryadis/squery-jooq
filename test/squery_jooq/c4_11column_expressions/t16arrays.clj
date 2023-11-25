(ns squery-jooq.c4-11column-expressions.t16arrays
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
    (org.jooq SQLDialect DSLContext Field Select Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL QOM$Lateral SelectImpl)
    (org.jooq.conf Settings StatementType)))

;(connect "mysql")
(connect "postgres")

;;arrays all members same type, else i use json-arrays

(ps [(array 1 2 3 4)
     ;;from table result of subquery
     (qarray (s [(unwind-array (array 1 2 3 4))]))])

(ps [(acount (array 1 2 3))
     (aget (array 1 2 3) 2)
     (type (aget (array 1 2 3) 2))])

(ps [(adissoc (array 1 2 3) 2)
     (aassoc (array 1 2 3) 1 -10)
     (aconcat (array 1 2 3) (array 4 5 6) (array 7 8))
     (aand (array 1 2 3) (array 2))])

(ps [(amap (fn [:x] (+ :x 1)) (array 1 2 3))
     (afilter (fn [:x] (> :x 1)) (array 1 2 3))])