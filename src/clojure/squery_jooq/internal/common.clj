(ns squery-jooq.internal.common
  (:require [squery-jooq.utils.general :refer [keyword-map string-map]]
            clojure.set
            [clojure.core :as c]
            [squery-jooq.state :refer [ctx]])
  (:import (org.jooq.impl DSL)
           (org.jooq Field Table JSONEntry Row Row1 Row2 Row3 Row4 Row5 Row6)))

;;the first 2 functions is to use
;;  keyword instead(col ...)
;;  simple literals instead (lit ...)
;;the first replaces only keywords
;;the second replaces keywords and numbers,strings to (lit  )

#_(defn get-field-sql [field]
  (let [query (-> @ctx (.select field))]
    (loop [sql-string  (.getSQL query)
           bind-values (.getBindValues query)]
      (if (c/empty? bind-values)
        (str " ( " (c/subs sql-string 7) " ) ")
        (let [cur-bind-value (c/first bind-values)
              cur-bind-value (if (c/string? cur-bind-value)
                               (c/str "'" cur-bind-value "'")
                               cur-bind-value)]
          (recur (.replaceFirst sql-string "\\?" (String/valueOf cur-bind-value))
                 (rest bind-values)))))))

(defn get-sql [query]
  (loop [sql-string  (.getSQL query)
         bind-values (.getBindValues query)]
    (if (c/empty? bind-values)
      sql-string
      (let [cur-bind-value (c/first bind-values)
            cur-bind-value (if (c/string? cur-bind-value)
                             (c/str "'" cur-bind-value "'")
                             cur-bind-value)
            cur-bind-value (String/valueOf cur-bind-value)
            cur-bind-value (.replaceAll cur-bind-value "([\\\\^$|()\\[\\]{}.*+?])" "\\\\$1")]
        (recur (.replaceFirst sql-string "\\?" cur-bind-value)
               (rest bind-values))))))

(defn get-field-sql [field]
  (let [query (-> @ctx (.select field))
        sql-string (get-sql query)]
    (str " ( " (c/subs sql-string 7) " ) ")))

(declare columns)

(defn as-map? [m]
  (and (map? m)
       (= (count m) 1)
       (qualified-keyword? (first (keys m)))))

;;TODO make it faster protocol
;;A Condition can be turned into a Field<Boolean> using DSL.field
(defn column
  " converts always to column
    if keyword => column, else lit, but always column result
    and if {k v} converts the v to col,lit (if keyword,string,number)
    field keyword => col
    field map {k v} => (.as v (name k))
      v is also converted
      if keyword => col
      if not column => lit
      else v (no change)"
  [field]
  ;(prn field (type field) (instance? Field field))
  (cond

    (or (instance? Field field) (instance? Row field))
    field

    (keyword? field)
    (DSL/field (name field))

    (vector? field)
    (DSL/jsonbArray (into-array Field (columns field)))
    ;(DSL/array (into-array Field (columns field)))

    ;;new column
    (as-map? field)
    (let [k (name (first (keys field)))
          v (column (first (vals field)))]
      (.as ^Field v k))

    ;;json object
    (map? field)
    (let [pairs (into [] field)
          pairs (mapv (fn [p]
                        (.value (DSL/key (name (first p)))
                                (column (second p))))
                      pairs)]
      (DSL/jsonbObject (into-array JSONEntry pairs)))

    :else
    (DSL/val field)))

(defn columns [fields]
  (mapv column fields))

(defn row-internal
  ([field] (DSL/row (column field)))
  ([field1 field2] (DSL/row (column field1) (column field2)))
  ([field1 field2 field3] (DSL/row (column field1) (column field2) (column field3)))
  ([field1 field2 field3 field4] (DSL/row (column field1) (column field2) (column field3) (column field4)))
  ([field1 field2 field3 field4 field5] (DSL/row (column field1) (column field2) (column field3) (column field4) (column field5)))
  ([field1 field2 field3 field4 field5 field6] (DSL/row (column field1)
                                                        (column field2)
                                                        (column field3)
                                                        (column field4)
                                                        (column field5)
                                                        (column field6))))

(defn values-internal [rows-vec]
  (c/cond
    (c/= (c/count (c/first rows-vec)) 1)
    (DSL/values (c/into-array Row1 (c/map #(apply row-internal %) rows-vec)))

    (c/= (c/count (c/first rows-vec)) 2)
    (DSL/values (c/into-array Row2 (c/mapv #(apply row-internal %) rows-vec)))

    (c/= (c/count (c/first rows-vec)) 3)
    (DSL/values (c/into-array Row3 (c/mapv #(apply row-internal %) rows-vec)))

    (c/= (c/count (c/first rows-vec)) 4)
    (DSL/values (c/into-array Row4 (c/mapv #(apply row-internal %) rows-vec)))

    (c/= (c/count (c/first rows-vec)) 5)
    (DSL/values (c/into-array Row5 (c/mapv #(apply row-internal %) rows-vec)))

    (c/= (c/count (c/first rows-vec)) 6)
    (DSL/values (c/into-array Row6 (c/mapv #(apply row-internal %) rows-vec)))

    :else
    (throw (Exception. "Values don't support the number of fields."))))

;;(pq (.as (values [1 "a"] [2 "b"])
;         "t" (into-array String ["a" "b"])))

(defn table [t]
  (cond
        (or (keyword? t) (string? t))
        (DSL/table (name t))

        (vector? t)
        (let [schema (first t)
              table-values (into [] (rest t))
              table-name (first schema)
              table-columns (mapv (fn [c] (if (keyword? c)
                                            (name c)
                                            c))
                                  (rest schema))]
          (.as (values-internal table-values)
               (name table-name)
               (into-array String table-columns)))

        (map? t)
        (let [k (first (keys t))
              v (table (first (vals t)))]
          (.as ^Table v (name k)))

        :else
        t))

(defn tables [ts]
  (mapv table ts))

(defn single-maps
  "Makes all map members to have max 1 pair,and key to be keyword(if not starts with $) on those single maps.
   [{:a 1 :b 2} 20 {'c' 3} [1 2 3]] => [{:a 1} {:b 2} 20 {:c 3} [1 2 3]]
   It is used from read-write/project/add-fields
   In commands its needed ONLY when i want to seperate command options from extra command args.
   (if i only need command to have keywords i use command-keywords function)"
  ([ms keys-to-seperate]
   (loop [ms ms
          m  {}
          single-ms []]
     (if (and (empty? ms)
              (or (nil? m)                                  ; last element was not a map
                  (and (map? m) (empty? m))))               ; last element was a map that emptied
       single-ms
       (cond

         (not (map? m))
         (recur (rest ms) (first ms) (conj single-ms m))

         (empty? m)
         (recur (rest ms) (first ms) single-ms)

         ; if keys-to-seperate
         ;   and map doesnt have any key that needs seperation,keep it as is
         (and (not (empty? keys-to-seperate))
              (empty? (clojure.set/intersection (set (map (fn [k]
                                                            (if (string? k)
                                                              (keyword k)
                                                              k))
                                                          (keys m)))
                                                keys-to-seperate)))
         (recur (rest ms) (first ms) (conj single-ms (keyword-map m)))

         :else
         (let [[k v] (first m)]
           (recur ms (dissoc m k) (conj single-ms (keyword-map {k v}))))))))
  ([ms]
   (single-maps ms #{})))

(defn string-keys-column-values [m]
  (reduce (fn [new-m k]
            (assoc new-m (name k) (column (get m k))))
          {}
          (keys m)))

(defn sort-arguments [cols]
  (mapv (fn [col]
          (let [desc? (and (keyword? col) (clojure.string/starts-with? (name col) "!"))
                nl?   (and (keyword? col) (clojure.string/ends-with? (name col) "!"))
                col (if desc? (keyword (subs (name col) 1)) col)
                col (if nl? (keyword (subs (name col) 0 (dec (count (name col))))) col)
                col (column col)                            ;; TODO was column-keyword
                col (cond
                      (and desc? nl?)
                      (.nullsLast (.desc ^Field  col))
                      ;(.desc_nulls_last  col)

                      desc?
                      (.desc col)

                      nl?
                      (.nullsLast (.asc col))
                      ;(.asc_nulls_last  col)

                      :else
                      col)]
            col))
        cols))