(ns squery-jooq.stages
  (:refer-clojure :exclude [sort distinct])
  (:require [squery-jooq.internal.common :refer [columns column table tables sort-arguments single-maps]]
            [squery-jooq.internal.query :refer [pipeline switch-select-from]]
            [squery-jooq.state :refer [ctx]]
            [squery-jooq.utils.general :refer [nested2]])
  (:import (org.jooq.impl DSL DefaultDSLContext SelectImpl)
           (org.jooq DerivedColumnList Name SelectFieldOrAsterisk Select SelectQuery GroupField Field Condition)))



;;q without the enviroment for internal use only, for stages that call subqueries like union
(defmacro sub-q-internal [& qforms]
  (let [qforms (switch-select-from qforms true)
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)
        ;_ (prn "query" query)
        ]
    query))


(defn select [^org.jooq.impl.DefaultDSLContext df fields]
  (let [asterist? (first (filter (fn [f] (or (= f :*) (= f "*"))) fields))
        fields (filter (fn [f] (not (or (= f :*) (= f "*")))) fields)
        distinct? (first (filter (fn [f] (= f :distinct)) fields))
        fields (filter (fn [f] (not= f :distinct)) fields)]
    (if (empty? fields)
      (if distinct?
        (.selectDistinct df (into-array SelectFieldOrAsterisk [(DSL/asterisk)]))
        (.select df (into-array SelectFieldOrAsterisk [(DSL/asterisk)])))
      (if asterist?
        (if distinct?
          (.selectDistinct (.selectDistinct df (into-array SelectFieldOrAsterisk [(DSL/asterisk)])) (columns fields))
          (.select (.select df (into-array SelectFieldOrAsterisk [(DSL/asterisk)])) (columns fields)))
        (if distinct?
          (.selectDistinct df (columns fields))
          (.select df (columns fields)))))))


;;from args (from can take many args, like many tables etc)
;;squery args
;; :author   "author"
;;  a map with allias for a table {:a :author}
;;  table from schema(first=table name)-values  [[:t :d :c]  [1 "a"]  [2 "b"]]
;;else any jooq arg like the below (squery doesnt change it)
;;  table from another query (q [[:t :d :c] [1 "a"] [2 "b"]] "nested")
(defn from
  ([df tables-exp]
   (.from df (tables tables-exp)))
  ;;dummy never used
  ([tables-exp]
   (from @ctx tables-exp)))

;;used out of the sql query stored in variables page 97 jooq
;;i will use it for The WITH RECURSIVE clause
#_(defn with-explicit [df table-schema query]
  (let [table-name (name (first table-schema))
        fields (mapv name (rest table-schema))
        table-expr (-> ^Name (DSL/name (into-array String [table-name]))
                       ^DerivedColumnList
                       (.fields (into-array Name (mapv #(DSL/name %) fields)))
                       (.as query))]
    (.with df table-expr)))

(defn with
  ([ctx table-name query]
   (.as (.with ctx (name table-name)) query))
  ;;dummy never used
  ([table-name query]
   (with @ctx table-name query)))

(defn where [df fs]
  (.where df fs))

(defn having [df fs]
  (.having df (into-array Condition (columns fs))))

(defn limit [df n]
  (.limit df n))

(defn skip [df n]
  (.offset df n))

(defn sort
  "DataFrame orderBy"
  [df & cols]
  (.orderBy df (sort-arguments cols)))

;;after sort and after limit,keeps all if same value based on sorting
(defn with-ties [df]
  (.withTies df))

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

;;---------------------------------group----------------------------------


(defn group [^Select df & cols]
  (.groupBy df (into-array GroupField (columns cols))))


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

(defn and-internal [cols]
  (nested2 #(.and (column %1) (column %2))  (if (= (count cols) 1)
                                              (concat cols [(column true)])
                                              cols)))

(defn join
  "inner join
   call
   (join :author (= :book.author_id :author.id))
   (join :author :id)"
   [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.join (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.join (table df1) (table df2))
         (if (> (count join-conditions-or-fields) 1)
           (and-internal join-conditions-or-fields)
           (first join-conditions-or-fields)))))

(defn left-outer-join
  "keep left always(right null if no join) + right that joined"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.leftOuterJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.leftOuterJoin (table df1) (table df2))
         (if (> (count join-conditions-or-fields) 1)
           (and-internal join-conditions-or-fields)
           (first join-conditions-or-fields)))))

(defn right-outer-join
  "keep right always(left null if no join) + left that joined"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.leftOuterJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.leftOuterJoin (table df1) (table df2))
         (if (> (count join-conditions-or-fields) 1)
           (and-internal join-conditions-or-fields)
           (first join-conditions-or-fields)))))

(defn full-join
  "keep both always, null if not join, else join values"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.fullJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.fullJoin (table df1) (table df2))
         (if (> (count join-conditions-or-fields) 1)
           (and-internal join-conditions-or-fields)
           (first join-conditions-or-fields)))))

(defn left-semi-join
  "keep left that would join, but do not join"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.leftSemiJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.leftSemiJoin (table df1) (table df2))
         (if (> (count join-conditions-or-fields) 1)
           (and-internal join-conditions-or-fields)
           (first join-conditions-or-fields)))))

(defn left-anti-join
  "keep left that wouldn't join, do not join"
  [df1 df2 & join-conditions-or-fields]
  (if (empty? (filter #(instance? Condition %) join-conditions-or-fields))
    (.using (.leftAntiJoin (table df1) (table df2)) (into-array Field (columns join-conditions-or-fields)))
    (.on (.leftAntiJoin (table df1) (table df2))
         (if (> (count join-conditions-or-fields) 1)
           (and-internal join-conditions-or-fields)
           (first join-conditions-or-fields)))))

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













