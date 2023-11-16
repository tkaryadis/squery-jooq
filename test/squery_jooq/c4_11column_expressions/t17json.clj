(ns squery-jooq.c4-11column-expressions.t17json
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q pq s ps]]
            [squery-jooq.state :refer [connect-mysql connect-postgres ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import
    (java.sql Timestamp)
    (java.time Instant)
    (java.util Date)
    (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL)
    (org.jooq.conf Settings StatementType)))

(connect-mysql SQLDialect/MYSQL)

#_(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

;;so far i used
;;vectors
;;  stage project
;;  stage from  (literal table)
;;maps
;;  operator .as (size=1, key=keyword)
;;  stage from (as table alias)

;;only problem is the .as that is operator as map, if size=1 and key=keyword
;;the stages dont cause problems

;;there are 2 array types
;;  json
;;  normal
;;by default i use json ones, the literal [1 3 6] becomes json-array

;;json => keyword keys only if size > 1, else string keys works always
(ps [{::a 1}             ;;;qualifies keyword => .as
     {:a 1}            ;;keys can be keywords or strings
     {:a 1 "b" 2}
     [1 2 3]              ;;json-array
     {:a [3 4 5] :b 4}  ;;nested array
     [{:a 1} {:a 2}]    ;;array of docs
     [{:a {:b 2}}]      ;;nested object

     {:a [{"b" 20} {"b" 30} {"b" 100}]}
     ])

(ps [(json-array 1 2 3)])
(ps [(json-object "a" 1 "b" 2 :c 3)])


;(ps [(assoc {:a 1} :b 2)])     ;;add or update
;(ps [(dissoc {:a 1} :a)])
;(ps [(assoc-insert {:a 1} :b 2)])    ;;only add
;(ps [(assoc-update {:a 1} :a 2)])    ;;only update

#_(ps [(keys {:a 1 :b 2})])

#_(ps [(get [1 2 3] 2)
     (get {:a 20} :a)
     (get-in [1 2 3] [2])
     (get-in {"a" [{:b 20} {"b" 30} {"b" 100}]}       ;;get-in
             [:a 0 "b"])
     (get-in {"a" [{"b" [1 2 5]} {"b" 30} {"b" 100}]}       ;;get-in and cast
             ["a" 0 "b" 2]
             :int)])

;;json-functions done

;;see also aggregate functions to collect to json-array or json-object

;;mysql no, postgres ok
(pq [[:t :a :b] [1 2] [10 20]]
    [(conj-each :a)])

;;mysql+postgress ok
#_(pq [[:t :a :b] ["1" 2] ["2" 20]]
    [(merge-acc :a :b)])