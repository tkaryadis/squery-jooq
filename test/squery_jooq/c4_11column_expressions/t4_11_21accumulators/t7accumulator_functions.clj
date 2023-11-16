(ns squery-jooq.c4-11column-expressions.t4-11-21accumulators.t7accumulator-functions
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

;;i use group even if empty, to avoid problems

(pq :book)

;;picks a random id as value (in mysql group was needed)
(pq :book
    (group)
    [(rand-acc :id)])

;;array accumulators

;;Field<Integer> jsonLengthField = field("JSON_LENGTH(json_column)", Integer.class);
;
;Result<Record> result = context
;    .select(jsonLengthField)
;    .from(table("your_table"))
;    .fetch();

(pq [[:t :a] [{:a 1 :b 2}] [{:a 2 :b 3 :c 4}]]
    [(DSL/field "JSON_LENGTH(a)")])

#_(pq :book
    (group)
    [(and-acc (< :id 4))
     (and-acc (< :id 5))])