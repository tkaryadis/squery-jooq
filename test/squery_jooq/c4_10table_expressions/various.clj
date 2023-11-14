(ns squery-jooq.tables.various
  (:refer-clojure :only [])
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q insert uq dq pq s]]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.printing :refer [print-results print-sql]])
  (:refer-clojure)
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk Results Row RowN Row1 Row2 Row3)
           (org.jooq.conf Settings StatementType)
           (org.jooq.impl DSL)
           (java.util Arrays)))

(connect (slurp "/home/white/IdeaProjects/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

;;Author a = AUTHOR.as("a");
;Book b = BOOK.as("b");
;// Use aliased tables in your statement
;create.select()
;.from(a)
;.join(b).on(a.ID.eq(b.AUTHOR_ID))
;.where(a.YEAR_OF_BIRTH.gt(1920)
;.and(a.FIRST_NAME.eq("Paulo")))
;.orderBy(b.TITLE)
;.fetch();

(pq {:a :author}
    (join {:b :book}
            (= :a.id :b.author_id))
    ((> :a.year_of_birth 1920) (= :a.first_name "Paulo"))
    (sort :b.title))

;;create.select()
;.from(values(row(1, "a"),
;row(2, "b")).as("t", "a", "b"))
;.fetch();

(pq [[:t :d :c]
     [1 "a"]
     [2 "b"]])


;;Table<?> nested =
;create.select(BOOK.AUTHOR_ID, count().as("books"))
;.from(BOOK)
;.groupBy(BOOK.AUTHOR_ID).asTable("nested");

;create.select(nested.fields())
;.from(nested)
;.orderBy(nested.field("books"))
;.fetch();


(def nested (q :book
               (group :author_id)
               [:author_id {:books (count-acc)}]
               "nested"))

(pq nested
    (sort (.field nested "books"))
    (select (.fields nested)))