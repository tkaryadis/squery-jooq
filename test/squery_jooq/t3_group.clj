(ns squery-jooq.t3-group
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