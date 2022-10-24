(ns squery-jooq.stages
  (:refer-clojure :exclude [sort distinct])
  (:require [squery-jooq.internal.common :refer [columns column table tables sort-arguments single-maps]]
            [squery-jooq.internal.query :refer [pipeline switch-select-from]]
            [squery-jooq.state :refer [ctx]]
            [squery-jooq.utils.general :refer [nested2]])
  (:import (org.jooq.impl DSL DefaultDSLContext SelectImpl)
           (org.jooq SelectFieldOrAsterisk Select SelectQuery GroupField Field Condition)))



;;q without the enviroment for internal use only, for stages that call subqueries like union
(defmacro sub-q-internal [& qforms]
  (let [qforms (switch-select-from qforms true)
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)
        ;_ (prn "query" query)
        ]
    query))

;;project(select) stage can be 3 ways
;; col
;; add new coll
;;   anonymous
;;   with new name
;; [:CustomerID (lit 5) {:price (coll "UnitPrice")}]
;; (.select ^Dataset (into-array [(col "CustomerID")  (lit 5) (.as (col "UnitPrice") "price")]))
;;TODO select can take also arguments like "*" in spark
;;i can add new coloumns with {} or just with literals
(defn select
  "[:CustomerID (lit 5) {:price (coll \"UnitPrice\")}]
   (.select ^Dataset (into-array [(col \"CustomerID\")  (lit 5) (.as (col \"UnitPrice\") \"price\")]))"
  [^org.jooq.impl.DefaultDSLContext df fields]
  (if (empty? fields)
    (.select df (into-array SelectFieldOrAsterisk [(DSL/asterisk)]))
    (.select df (columns fields))))


(defn from [df tables-exp]
  (.from df (tables tables-exp)))

(defn where [df fs]
  (.where df fs))

(defn having [df fs]
  (.having df (into-array Condition (columns fs))))

(defn group [df & cols]
  (.groupBy df (into-array Field (columns cols))))

(defn limit [df n]
  (.limit df n))

(defn skip [df n]
  (.offset df n))

(defn sort
  "DataFrame orderBy"
  [df & cols]
  (.orderBy df (sort-arguments cols)))

(defn union [df1 df2]
  (if (keyword? df2)
    (.union df1 (sub-q-internal df2))
    (.union df1 df2)))

(defn union-all [df1 df2]
  (if (keyword? df2)
    (.unionAll df1 (sub-q-internal df2))
    (.unionAll df1 df2)))

(defn except [df1 df2]
  (if (keyword? df2)
    (.except df1 (sub-q-internal df2))
    (.except df1 df2)))

(defn except-all [df1 df2]
  (if (keyword? df2)
    (.exceptAll df1 (sub-q-internal df2))
    (.except df1 df2)))

(defn intersection [df1 df2]
  (if (keyword? df2)
    (.intersection df1 (sub-q-internal df2))
    (.intersection df1 df2)))

;;---------------------------------joins----------------------------------

;;types
;;CROSS JOIN: A cross product
;;INNER JOIN: A cross product filtering on matches
;;OUTER JOIN: A cross product filtering on matches, additionally producing some unmatched rows
;;SEMI JOIN: A check for existence of rows from one table in another table (using EXISTS or IN)
;;ANTI JOIN: A check for non-existence of rows from one table in another table (using NOT EXISTS
;;           or some conditions NOT IN)

;;join predicates types
;;ON: Expressing join predicates explicitly
;;ON KEY: Expressing join predicates explicitly or implicitly based on a FOREIGN KEY (requires code generation)
;;USING: Expressing join predicates implicitly based on an explicit set of shared column names in
;;       both tables
;;NATURAL: Expressing join predicates implicitly based on an implicit set of shared column names
;;       in both tables

(defn- and-internal [cols]
  (nested2 #(.and (column %1) (column %2)) cols))

(defn join
  "inner join"
   [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.join (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.join (table df1) (table df2)) (and-internal join-conditions-or-fields))))

(defn left-outer-join
  "keep left always(right null if no join) + right that joined"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.leftOuterJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.leftOuterJoin (table df1) (table df2)) (and-internal join-conditions-or-fields))))

(defn right-outer-join
  "keep right always(left null if no join) + left that joined"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.leftOuterJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.leftOuterJoin (table df1) (table df2)) (and-internal join-conditions-or-fields))))

(defn full-join
  "keep both always, null if not join, else join values"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.fullJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.fullJoin (table df1) (table df2)) (and-internal join-conditions-or-fields))))

(defn left-semi-join
  "keep left that would join, but do not join"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.leftSemiJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.leftSemiJoin (table df1) (table df2)) (and-internal join-conditions-or-fields))))

(defn left-anti-join
  "keep left that wouldn't join, do not join"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.leftAntiJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.leftAntiJoin (table df1) (table df2)) (and-internal join-conditions-or-fields))))

(defn cross-join
  "cartesian product"
  [df1 df2]
  (.crossJoin (table df1) (table df2)))

(defn natural-join
  "natural join, join in all shared column names"
  [df1 df2]
  (.naturalJoin (table df1) (table df2)))

;;------------------------update-stages----------------------------------------------

(defn set-columns [utable update-map]
  (let [update-map (single-maps [update-map])]
    (reduce (fn [v t]
              (let [k (first (keys t))
                    vl (first (vals t))]
                (.set v (column k) vl)))
            utable
            update-map)))













