(ns squery-jooq.reactive
  (:use squery-jooq.reactor-utils.functional-interfaces)
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q pq s ps]]
            [squery-jooq.commands.update :refer [insert insert-values]]
            [squery-jooq.commands.admin :refer [create-table-if-not-exist drop-table-if-exists]]
            [squery-jooq.state :refer [connect-postgres-reactive ctx]]
            [squery-jooq.printing :refer [print-results print-sql]]
            [squery-jooq.reactor-utils.jooq :refer [flux-records]])
  (:refer-clojure)
  (:import (org.jooq.impl DSL SQLDataType)
           (reactor.core.publisher Flux Mono)))

(connect-postgres-reactive
  (read-string
    (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/reactive")))

(-> (drop-table-if-exists :temptable)
    (.blockFirst))

(-> (create-table-if-not-exist
      :temptable
      [[:id (-> (SQLDataType/BIGINT)
                (.nullable false)
                (.identity true))]
       :first
       :last]
      [(DSL/primaryKey (into-array String ["id"]))])
    (.blockFirst))

(comment
(-> (insert-values :temptable [:first :last] [["first1" "last1"] ["first2" "last2"]])
    (.blockFirst))

(-> ^Flux (q :temptable)
    (.collectList)
    ^Mono (.doOnNext (cfn [x] (prn x)))
    (.block))

(-> (insert-values :temptable [:first :last] [["first3" "last3"] ["first4" "last4"]] [:first])
    (.doOnNext (cfn [x] (prn x)))
    (.blockFirst))

)

(-> (insert-values :temptable [:first :last] [["first5" "last5"]] [:first])
    (.doOnNext (cfn [x] (prn x)))
    (.blockFirst))


(-> (insert :temptable
            [:first :last]
            [{"first" "first6" "last" "last6"}])
    (.doOnNext (cfn [x] (prn x)))
    (.blockFirst))

#_(-> (insert :temptable
            [:first :last]
            [{"first" "first7" "last" "last7"}]
            [:first])
    (.doOnNext (cfn [x] (prn x)))
    (.blockFirst))



(.read (System/in))