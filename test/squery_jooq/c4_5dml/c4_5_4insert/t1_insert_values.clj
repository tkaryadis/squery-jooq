(ns squery-jooq.c4_5dml.c4_5_4insert.t1-insert-values
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q pq]]
            [squery-jooq.commands.update :refer [insert insert-values]]
            [squery-jooq.commands.admin :refer [drop-table drop-table-if-exists create-table]]
            [squery-jooq.state :refer [connect-postgres ctx]]
            [squery-jooq.printing :refer [print-results print-sql]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL SQLDataType)
           (org.jooq.conf Settings StatementType)))

(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

(drop-table-if-exists :temptable)
(create-table :temptable
              [[:id (-> (SQLDataType/INTEGER)
                        (.nullable false)
                        (.identity true))]
               :first
               :last]
              [(DSL/primaryKey (into-array String ["id"]))])

(pq :temptable)

(insert-values :temptable
               [:first :last]
               [["first1" "last1"]
                ["first2" "last2"]])

(pq :temptable)

;;order can be any on header, i just keep the same on values also
(insert-values :temptable
               [:last :first]
               [["last3" "first3"]])

(pq :temptable)

;;returning fields of the rows that were inserted
(insert-values :temptable
               [:last :first]
               [["last3" "first3"]]
               [:last])

(pq :temptable)

(insert :temptable
        [{:last "last4" :first "first4"}
         {:first "first5" :last "last5"}])

(pq :temptable)

;;returning maps, wit the fields that i want, from the inserted docs
(insert :temptable
        [{:last "last4" :first "first4"}
         {:first "first5" :last "last5"}]
        [:first])