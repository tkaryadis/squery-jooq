(ns squery-jooq.c4-11column-expressions.t3to5types-cast
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q pq s ps]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect (slurp "/home/white/IdeaProjects/squery/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))


;;there are many for type checks + casting
(pq [[:t :a :b] [1 2] [3 4]]
    [(string :a)
     (long :b)
     {:c (str "Hello : " (cast :b :string))}
     (true? (= :b 2))])


;;TODO see more of types based on each db

;;4.11.4. Datatype coercions ??
;;4.11.5. Readonly columns  ??