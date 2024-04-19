(ns squery-jooq.c4-11column-expressions.t22window-functions
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

;;group no fields => 1 aggregation per table
;;group with fields => 1 aggregation per group
;;window functions => 1 aggregation per row

;;count().over(partitionBy(BOOK.AUTHOR_ID)))
;;rowNumber().over(orderBy(BOOK.ID.desc())))

;;find the group that the current :a belongs, and count (:a for each row)
(pq [[:t :a] [1] [1] [2] [3] [3] [3]]
    [:a
     (window (count-acc) (ws-group :a))])

;;sort by :!a and find the row number of the current row
(pq [[:t :a] [1] [1] [2] [3] [3] [3]]
    [:a
     (window (row-number) (ws-sort [:!a]))])


;;controlling window size

;;PRECEDING rows
;;current row     //i am here
;;Followind rows

;;Ways to limit the window frame
;;1)by a number of rows
;;   for example current+10 previous
;;2)by value range, compared to the current row
;;   for example [current value - 3, current value]
;;   if current row value = 10, then until value=7 all rows are included
;;3)by a number of groups?

;;count().over(orderBy(BOOK.PUBLISHED_IN).rangePreceding(42))

;;rows-preceding 2 means = max 2
;;so max count can be 3 (2+current row)
(pq [[:t :a] [1] [1] [2] [3] [3] [3] [4] [5] [6] [6]]
    [:a
     (rows-preceding (window (count-acc) (ws-sort [:a]))
                     2)
     ;;[current-value-range_V, current-row]
     ;;if i have same values, like the 3 threes, they all get the max
     ;;for example for 4 i include [4-2,4] => [2,4] = 5
     ;;for 3(like last), [3-2,3] = [1,last-3]
     ;;if negative or zero , i imagine one more on the left
     ;;=> 2 has 3 because zero
     (range-preceding (window (count-acc) (ws-sort [:a]))
                      2)
     ;;groups-preceding =1 means, rows from 1 previous group+current row
     ;;again if same value, all get the value that the last has
     ;;for example 4 = 3 rows from group 3, and 1 more the current row
     ;;(?not sure)
     (groups-preceding (window (count-acc) (ws-sort [:a]))
                       1)])


;;TODO has many more

