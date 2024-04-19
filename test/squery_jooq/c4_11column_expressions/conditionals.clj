(ns squery-jooq.c4-11column-expressions.conditionals
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer [q  pq s ps]]
            [squery-jooq.commands.update :refer [insert uq dq]]
            [squery-jooq.state :refer [connect-postgres ctx]]
            [squery-jooq.printing :refer [print-results print-sql]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)
           (org.jooq.conf Settings StatementType)))

(connect-postgres (slurp "/home/white/IdeaProjects/squery/squery-jooq/authentication/connection-string"))

;;0,false
;;1,true
;;NULL or UNKNOWN

;;[ANY] = NULL yields NULL (not FALSE)
;;[ANY] != NULL yields NULL (not TRUE)
;;NULL = NULL yields NULL (not TRUE)
;;NULL != NULL yields NULL (not FALSE)

(pq :book)
(pq :author)

;;not,and,or etc

;BOOK.TITLE.eq("Animal Farm").or(BOOK.TITLE.eq("1984"))
;.andNot(AUTHOR.LAST_NAME.eq("Orwell"))

;;empty results
(pq :book
    (join :author (= :book.author_id :author.id))
    ((or (= :title "Animal Farm")
           (= :title "1984"))
       (not= :last_name "Orwell")))

;;if
(pq :book
    [(if- (> :published_in 1988) 1 2)])

;;when(AUTHOR.FIRST_NAME.eq("Paulo"), "brazilian")
;.when(AUTHOR.FIRST_NAME.eq("George"), "english")
;.otherwise("unknown");

(pq :author
    [:first_name {:ethnicity (cond (= :first_name "Paulo") "brazilian"
                                   (= :first_name "George") "english"
                                   :else "unknown")}])

(pq :author
    [:first_name {:ethnicity (cond true "brazilian"
                                   (= :first_name "George") "english"
                                  :else "unknown")}])

;;exists for subqueries results
(ps [{:a (exists? (q :book (limit 0)))}])
(ps [{:a (not (exists? (q :book (limit 0))))}])

;;row(AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME).eq("George", "Orwell")
(pq :author
    [{:a (= (row :author.first_name :author.last_name) (row "George" "Orwell"))}])

;;BOOK.PUBLISHED_IN.between(1920).and(1940)
(pq :book
    ((<> :published_in 1948 1988)))

;BOOK.PUBLISHED_IN.notBetween(1920).and(1940)

(pq :book
    ((not (<> :published_in 1948 1988))))

;;row(BOOK.ID, BOOK.TITLE).between(1, "A").and(10, "Z");

(pq :book
    ((<> (row :id :title) (row 1 "A") (row 10 "Z"))))


