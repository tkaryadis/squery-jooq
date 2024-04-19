(ns squery-jooq.internal.admin
  (:require [squery-jooq.schema :refer [schema-types]])
  (:import (org.jooq Constraint CreateTableElementListStep)
           (org.jooq.impl DSL)))

(defn get-column-type [col]
  (if (keyword? (second col))
    (get schema-types (second col))
    (second col)))

(defn nillable-column [col]
  (cond
    (< (count col) 3)
    0

    (and (boolean? (nth col 2)) (true? (nth col 2)))
    1

    (and (boolean? (nth col 2)) (false? (nth col 2)))
    2

    :else
    0))

(defn add-table-columns [table-obj cols]
  (prn cols)
  (loop [cols cols]
    (if (empty? cols)
      table-obj
      (let [cur-col (first cols)
            cur-col (if (keyword? cur-col)
                      [cur-col :string]
                      cur-col)
            cur-col-name (name (first cur-col))
            cur-col-type (get-column-type cur-col)
            nillable (nillable-column cur-col)
            ;_ (prn "name" cur-col-name)
            ;_ (prn "type" cur-col-type)
            ;_ (prn "nillable" nillable)
            ]
        (recur (do
                 (cond
                   (= nillable 0)
                   (.column table-obj cur-col-name cur-col-type)

                   (= nillable 1)
                   (.column table-obj cur-col-name (.null_ cur-col-type))

                   (= nillable 2)
                   (.column table-obj cur-col-name (.notNull cur-col-type)))
                 (rest cols)))))))

(defn add-table-constraints [table-obj constraints]
  (loop [constraints constraints]
    (if (empty? constraints)
      table-obj
      (let [cur-constraint ^Constraint (first constraints)]
        (recur (do
                 (.constraints ^CreateTableElementListStep table-obj (into-array Constraint cur-constraint))
                 (rest constraints)))))))

