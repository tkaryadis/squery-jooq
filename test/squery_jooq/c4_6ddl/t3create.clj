(ns squery-jooq.c4-6ddl.t3create
  (:require [squery-jooq.operators :refer :all]
            [squery-jooq.stages :refer :all]
            [squery-jooq.commands.query :refer :all]
            [squery-jooq.commands.update :refer :all]
            [squery-jooq.commands.admin :refer :all]
            [squery-jooq.state :refer [connect ctx]]
            [squery-jooq.schema :refer [schema-types]]
            [squery-jooq.printing :refer [print-results print-sql ]]
            [clojure.core :as c])
  (:refer-clojure)
  (:import
    (java.sql Timestamp)
    (java.time Instant)
    (java.util Date)
    (org.jooq DataType SQLDialect DSLContext Field Select Table SelectFieldOrAsterisk)
    (org.jooq.impl DSL QOM$Lateral SQLDataType SelectImpl)
    (org.jooq.conf Settings StatementType)))

;(connect "mysql")
(connect "postgres")

;;create db
;(create-database "mydb")

(create-indexes :author
                (index [:first_name :!last_name] {:name "myindex1"})
                (index [:!first_name] {:name "myindex2" :unique true}))

;;TODO covering + partial indexes page 176

;;TODO 4.6.3.5. CREATE PROCEDURE

#_(create-schema "new-schema")

;;TODO 4.6.3.7. CREATE SEQUENCE

(defn get-column-type [col]
  (let [col-type (if (keyword? col)
                   (get schema-types :string)
                   (get schema-types (second col) (second col)))
        options (get col)]
    ))


(defn get-column-name [col]
  (if (keyword? col)
    (name col)
    (first col)))



(defn add-table-columns [table-obj cols]
  (loop [cols cols]
    (if (empty? cols)
      table-obj
      (let [cur-col (first cols)
            cur-col-name (get-column-name cur-col)
            cur-col-type (get-column-type cur-col)
            nillable (nillable-column cur-col)]
        (recur (do
                 (cond
                   (= nillable 0)
                   (.column table-obj cur-col-name cur-col-type)

                   (= nillable 1)
                   (.column table-obj cur-col-name (.null_ cur-col-type))

                   (= nillable -1)
                   (.column table-obj cur-col-name (.notNull cur-col-type)))
                 (rest cols)))))))

(defn create-table [table-name cols]
  (-> ^DSLContext @ctx
      (.createTable (name table-name))
      (add-table-columns cols)))

;;(def my-manual-schema
;  (build-schema
;    [:DEST_COUNTRY_NAME
;     :ORIGIN_COUNTRY_NAME
;     [:count :long false (Metadata/fromJson "{\"hello\":\"world\"}")]]))

;;candidate keys i delcare them as unique
(create-table :mytable
              [:col
               ;;3rd arg is for nil?
               [:col :type/orValue :true-false-missing]]
              {:constraints [:constraint-arg1
                             :constraint-arg2
                             ;;primaryKey("column1")
                             ;;constraint("pk").primaryKey("column1")
                             ;;unique("column1"),
                             ;;unique("column2", "column3")
                             ;;constraint("fk").foreignKey("column1").references("other_table", "other_column1")
                             ;;check(field(name("column1"), INTEGER).gt(0))
                             ]})