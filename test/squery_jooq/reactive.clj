(ns squery-jooq.reactive
  (:use squery-jooq.reactor-utils.functional-interfaces)
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q pq s ps]]
            [squery-jooq.commands.update :refer [insert]]
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
       :name]
      [(DSL/primaryKey (into-array String ["id"]))])
    (.blockFirst))

(-> (insert :temptable [:name] [["takis"] ["kostas"]])
    (.blockFirst))

(-> (q :temptable)
    (.subscribe (cfn [x] (prn x))))

(.read (System/in))