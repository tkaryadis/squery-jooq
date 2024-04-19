(ns squery-jooq.c4-11column-expressions.t17json
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q  pq s ps]]
            [squery-jooq.commands.update :refer [insert uq dq]]
            [squery-jooq.state :refer [connect-postgres ctx]]
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
(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

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

;;keys => jsonb array
(ps [(get (keys {:a 1 :b 2}) 0)])

;;jsonb arrays don't have a type => i need manual casting
(ps [(get [1 2 3] 2 :long)
     (get {:a 20} :a)
     (get-in [1 2 3] [2])
     (get-in {"a" [{:b 20} {"b" 30} {"b" 100}]}       ;;get-in
             [:a 0 "b"])
     (get-in {"a" [{"b" [1 2 5]} {"b" 30} {"b" 100}]}       ;;get-in and cast
             ["a" 0 "b" 2]
             :int)])

;;postgres only operators
(pq [[:t :a :b] [1 2] [3 4]]
    [(merge {:a :a} {:b 2})
     (concat [1 2 3] [4 5 6])     ;;its just merge, that works with arrays also
     (assoc {:a 20} "b" 2)
     (dissoc {:a 2 :b 4} "a")])

;;using the get,get-in of jooq
(ps [(contains? {:a 2} "a")])

(pq [[:t :a :b] [1 2] [10 20]]
    [(conj-each :a)])

(pq [[:t :a :b] ["1" 2] ["2" 20]]
    [(merge-acc :a :b)])

(pq [[:t :a :b] [[1 2 3] 100] [[5 6] 200]]
    [(unwind :a) :b])

(pq [[:t :a] [[1 2 3]]]
  [(get :a 0)
   (type (get :a 0))
   (map (fn [:x] (+ (long :x) 1)) :a)
   (filter (fn [:x] (> (long :x) 1)) :a)
   (get (filter (fn [:x] (> (long :x) 1)) :a) 0)])

(pq [[:t :a] [[1 2 3]]]
    [(get :a 0 :long)
     (get-long :a 0)])