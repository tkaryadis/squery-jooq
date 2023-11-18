(ns squery-jooq.c4-11column-expressions.t14strings
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q pq s ps]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import
    (java.sql Timestamp)
    (java.time Instant)
    (java.util Date)
    (java.util.regex Pattern)
    (org.jooq SQLDialect DSLContext Field Select Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL QOM$Lateral SelectImpl)
    (org.jooq.conf Settings StatementType)))

;(connect "mysql")
(connect "postgres")

(ps [(str "hello" "you" "there")
     (str "hi" (space 2) "you")
     (repeat "hello!" 5)])

(ps [(subs "hello" 2)
     (subs "hello" 0 10)
     (sub-index "hello" "e")])

(ps [(split-get "hello,,there,,you" ",," 1)
     (to-string (DSL/date "2000-01-01") "YYYY/MM/DD")])

(ps [(replace-all "he123le456" #"\d+" "-")
     (replace "he123le456" #"\d+" "-")
     (replace-all-str "he123le1234" "123" "-")])

