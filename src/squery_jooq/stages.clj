(ns squery-jooq.stages
  (:refer-clojure :exclude [sort distinct])
  (:require [squery-jooq.internal.common :refer [columns column table tables sort-arguments single-maps]]
            [squery-jooq.internal.query :refer [pipeline switch-select-from]]
            [squery-jooq.state :refer [ctx]])
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

(defn join
  ([df1 df2 join-condition]
   (.on (.join (table df1) (table df2)) join-condition))
  ([df1 df2 join-condition join-type]
   #_(.join df1 (table df2) (column join-condition) (name join-type))))

(defn join-left-outer
  ([df1 df2 join-condition]
   (.on (.leftOuterJoin (table df1) (table df2)) join-condition)))

(defn join-cross
  "cartesian product"
  [df1 df2]
  (.crossJoin (table df1) (table df2)))

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
  (prn "dddd" df2)
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


;;------------------------update-stages----------------------------------------------

(defn set-columns [utable update-map]
  (let [update-map (single-maps [update-map])]
    (reduce (fn [v t]
              (let [k (first (keys t))
                    vl (first (vals t))]
                (.set v (column k) vl)))
            utable
            update-map)))













