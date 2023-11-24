(ns squery-jooq.c4-10table-expressions.t1-t11-derived-and-alias
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q sq pq s ss ps]]
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

;;---------------------------derived tables------------------------

;;there is a Table class that allows to do things like
;;  Table<?> named = table(select(AUTHOR.ID).from(AUTHOR)).as("t");

(def nested (q :book
               (group :author_id)
               [:author_id {::books (count-acc)}]
               :nested) ;;alias
  )

;;i can access it fields also like
;;(.fields nested)

(pq nested
    [:nested.author_id])

;;-------------------------generated-series---------------------------

(pq (table-range 0 10))

;;--------------------------alias-------------------------------------

(pq {::a :author}
    (join {::b :book}
          (= :a.id :b.author_id))
    ((> :a.year_of_birth 1920) (= :a.first_name "Paulo"))
    (sort :b.title))

;;-------------------------tables from row literals------------------

;;Derived column lists
;;table from schema+literal rows
(pq [[:t :d :c] [1 "a"] [2 "b"]]
    [:t.d])