(ns squery-jooq.pojo
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q pq s ps]]
            [squery-jooq.state :refer [connect-postgres ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import (com.mysql.cj.xdevapi SelectStatementImpl)

           (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)
           (testing Book)))

(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

(mapv #(prn (.toString %)) (.fetchInto (q :book) Book))