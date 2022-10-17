(ns squery-jooq.dml.select.t3-join
  (:require [squery-jooq.stages :refer :all]
            [squery-jooq.commands :refer [q]]
            [squery-jooq.state :refer [connect ctx]])
  (:import (org.jooq SQLDialect DSLContext Field Table SelectFieldOrAsterisk)
           (org.jooq.impl DSL)))

(connect (slurp "/home/white/IdeaProjects/squery-jooq/connection-string")
         SQLDialect/POSTGRES
         (-> (Settings.) (.withRenderFormatted true)))

(def query (q :book
              (join :author (= :book.author_id :author.id))
              ((= :book.published_in 1948))
              [:book.title :author.first_name :author.last_name]))

(prn (.getSQL query))

(mapv (fn [r] (prn (.intoMap r))) (.fetch query))

;;create.select()
;      .from(AUTHOR
;      .leftOuterJoin(BOOK
;        .join(BOOK_TO_BOOK_STORE)
;        .on(BOOK_TO_BOOK_STORE.BOOK_ID.eq(BOOK.ID)))
;      .on(BOOK.AUTHOR_ID.eq(AUTHOR.ID)))
;      .fetch();

(def jquery1 (-> @ctx
                 (.select (into-array SelectFieldOrAsterisk [(DSL/asterisk)]))
                 (.from (.on (.leftOuterJoin ^Table
                                             (DSL/table "author")
                                             (.on (.join ^Table (DSL/table "book")
                                                         (DSL/table "BOOK_TO_BOOK_STORE"))
                                                  (.eq (DSL/field "BOOK_TO_BOOK_STORE.BOOK_ID")
                                                       (DSL/field "AUTHOR.ID"))))
                             (.eq (DSL/field "BOOK.AUTHOR_ID")
                                  (DSL/field "AUTHOR.ID"))))))

(def query1 (q (leftOuterJoin :author
                              (join :book
                                    :BOOK_TO_BOOK_STORE
                                    (= :BOOK_TO_BOOK_STORE.BOOK_ID :BOOK.ID))
                              (= :BOOK.AUTHOR_ID :AUTHOR.ID))))

(prn (.getSQL query1))

(mapv (fn [r] (prn (.intoList r))) (.fetch query1))
