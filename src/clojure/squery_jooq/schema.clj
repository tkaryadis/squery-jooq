(ns squery-jooq.schema
  (:import (org.jooq DataType)
           (java.sql Date Timestamp)))

(def schema-types
  {
   ;:binary      binary
   :boolean   Boolean
   :byte      Byte
   :date      Date
   :double    Double
   :float     Float
   :integer   Integer
   :int       Integer
   :long      Long        ;;bigint
   ;:null      DataTypes/NullType
   :short     Short
   :string    String        ;;varchar
   :timestamp Timestamp
   })