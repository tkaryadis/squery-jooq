(ns squery-jooq.c4-6ddl.t4drop
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer :all]
            [squery-jooq.commands.update :refer :all]
            [squery-jooq.commands.admin :refer :all]
            [squery-jooq.state :refer [connect-postgres ctx]]
            [squery-jooq.schema :refer [schema-types]]
            [squery-jooq.printing :refer [print-results print-sql ]]
            [clojure.core :as c])
  (:refer-clojure)
  (:import
    (java.sql Timestamp)
    (java.time Instant)
    (java.util Date)
    (org.jooq Constraint ConstraintTypeStep CreateTableElementListStep DataType SQLDialect DSLContext Field Select Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL QOM$Lateral SQLDataType SelectImpl)
    (org.jooq.conf Settings StatementType)))

(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

;;drop i use interop if missing no clojure jooq wrapper function

;;clojure versions exists, for   database/index/table

;(create-table :book-copy (q :book))

(drop-table :book-copy)
;(drop-table :book-copy {:cascade true})

