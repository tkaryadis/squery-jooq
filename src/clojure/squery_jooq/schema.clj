(ns squery-jooq.schema
  (:import (org.jooq DataType)
           (java.sql Date Timestamp)
           (org.jooq.impl SQLDataType)))

(def schema-types
  {
   ;:binary      binary
   :boolean   SQLDataType/BOOLEAN
   :byte      SQLDataType/TINYINT
   :date      SQLDataType/DATE
   :double    SQLDataType/DOUBLE
   :float     SQLDataType/FLOAT
   :integer   SQLDataType/INTEGER
   :int       SQLDataType/INTEGER
   :long      SQLDataType/BIGINT
   ;:nil
   :short     SQLDataType/SMALLINT
   :string    SQLDataType/VARCHAR
   :timestamp SQLDataType/TIMESTAMP
   })