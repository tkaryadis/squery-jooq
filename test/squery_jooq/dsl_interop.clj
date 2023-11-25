(ns squery-jooq.dsl-interop
  (:require [squery-jooq.stages :refer :all]
            [squery-jooq.query :refer [q]]
            [squery-jooq.state :refer [connect ctx]])
  (:import (java.sql DriverManager)
           (org.jooq.impl DSL SelectImpl)
           (org.jooq SQLDialect DSLContext Field)))

(connect "postgres")

(def query (-> @ctx
               (.select [(DSL/field "first_name")])
               (.from [(DSL/table "author")])
               (.where [(.eq ^Field (DSL/field "first_name") "George")
                        (.eq ^Field (DSL/field "last_name") "Orwell")])))

(prn (type query))

(prn (.getSQL query))

(mapv (fn [r] (prn (.intoMap r))) (.fetch query))

(def query1 (-> @ctx
                (select [:first_name])
                (from [:author])
                (.where [(.eq ^Field (DSL/field "first_name") "George")
                         (.eq ^Field (DSL/field "last_name") "Orwell")])))

(prn (type query1))

(prn (.getSQL query1))

(mapv (fn [r] (prn (.intoMap r))) (.fetch query1))

(def query2 (q :author
               ((= :first_name "George"))
               ;[:first_name]
               #_(.where [(.eq ^Field (DSL/field "first_name") "George")
                          (.eq ^Field (DSL/field "last_name") "Orwell")])))

(prn (type query2))

(prn (.getSQL query2))

(mapv (fn [r] (prn (.intoMap r))) (.fetch query2))

;;create.select(AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME, count())
;.from(AUTHOR)
;.join(BOOK).on(BOOK.AUTHOR_ID.eq(AUTHOR.ID))
;.where(BOOK.LANGUAGE.eq("DE"))
;.and(BOOK.PUBLISHED_IN.gt(2008))
;.groupBy(AUTHOR.FIRST_NAME, AUTHOR.LAST_NAME)
;.having(count().gt(5))
;.orderBy(AUTHOR.LAST_NAME.asc().nullsFirst())
;.limit(2)
;.offset(1)
;.forUpdate()
;.fetch();

#_(def query1 (-> ctx
                  (.selectFrom (DSL/table "actor"))
                  (.where [(.eq ^Field (DSL/field "first_name") "PENELOPE")
                           (.eq ^Field (DSL/field "last_name") "GUINESS")])))


