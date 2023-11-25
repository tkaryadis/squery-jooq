(ns squery-jooq.dml.select.t1-select
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.query :refer [q pq s ps]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql ]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect (slurp "/home/white/IdeaProjects/squery/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

;;args of select are   org.jooq.SelectField
;;TODO 4.5.3.1.3. Tables as SelectField
;;org.jooq.Table extends org.jooq.SelectField => can be used in select

#_(ps [{:a 1} 10])

#_(pq :author)

#_(pq :author
    [:*])

;;this is empty select no *, i dont know the difference, page 102 SELECT *
#_(pq :author)

#_(pq :author
    [:author.id :author.first_name])

#_(pq :author
    [:distinguished :distinct])

;;TODO 4.5.3.1.7. SELECT DISTINCT ON



;;SELECT AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME, COUNT(*)
;FROM AUTHOR
;JOIN BOOK ON AUTHOR.ID = BOOK.AUTHOR_ID
;WHERE BOOK.TITLE = '1984'
;AND BOOK.PUBLISHED_IN > 2008
;GROUP BY AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME
;HAVING COUNT(*) > 5
;ORDER BY AUTHOR.LAST_NAME ASC NULLS FIRST
;LIMIT 2
;OFFSET 1

;;empty, based on sample data
#_(pq :author
    (join :book (= :author.id :book.author_id))
    ((= :book.title "1984") (> :book.published_in 2008))
    (group :author.first_name :author.last_name)
    ((> (count-acc) 5))
    (sort :author.last_name)
    (limit 2)
    (skip 1)
    [:author.first_name :author.last_name (count-acc)])

