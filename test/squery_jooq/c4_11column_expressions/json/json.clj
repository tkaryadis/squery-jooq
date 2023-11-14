(ns squery-jooq.columns.json
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q insert uq dq pq s ps]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql print-json-results]]
            [squery-jooq.internal.common :refer [column]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect (slurp "/home/white/IdeaProjects/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

(pq :book)

;;creating arrays,docs and nested
(print-results
  (q :book
     (limit 1)
     [{:a 1}             ;;;as, new name for the fields (1 key+keyword= as)
      {"a" 1}            ;;json-object with 1 field
      {:a 1 :b 2}        ;;json-object with >1 field (keywords or strings both works)
      {"a" 3 "b" 4}      ;;json-object with >1 field
      [1 2 3]              ;;array
      {"a" [3 4 5] "b" 4}  ;;nested array
      [{"a" 1} {"a" 2}]    ;;nested doc
      ]))

;;get index from array
(print-results
  (s [(get [1 2 3] 2)
      (get [{"a" 1} {"a" 2}] 1)]))


(print-results
  (s [(get-in-doc {"a" [{"b" 20} {"b" 30} {"b" 100}]}       ;;get-in
                  ["a" 0 "b"])
      (get-in-doc {"a" [{"b" 20} {"b" 30} {"b" 100}]}       ;;get-in and cast
                  ["a" 0 "b"]
                  :int)]))