(ns squery-jooq.reactive
  (:use squery-jooq.reactor-utils.functional-interfaces)
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q pq s ps]]
            [squery-jooq.state :refer [connect-postgres-reactive ctx]]
            [squery-jooq.printing :refer [print-results print-sql]]
            [squery-jooq.reactor-utils.jooq :refer [flux-records]])
  (:refer-clojure)
  (:import (reactor.core.publisher Flux)))

(connect-postgres-reactive
  (read-string
    (slurp "/home/white/IdeaProjects/squery/squery-jooq-reactive/authentication/reactive")))

(-> (flux-records (q :author))
    (.subscribe (cfn [x] (prn x))))

(.read (System/in))