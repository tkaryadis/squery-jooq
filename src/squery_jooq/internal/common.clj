(ns squery-jooq.internal.common
  (:require [squery-jooq.utils.general :refer [keyword-map string-map]]
            clojure.set)
  (:import (org.jooq.impl DSL)
           (org.jooq Field Table JSONEntry)))

;;the first 2 functions is to use
;;  keyword instead(col ...)
;;  simple literals instead (lit ...)
;;the first replaces only keywords
;;the second replaces keywords and numbers,strings to (lit  )

(declare columns)

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

    (instance? Field field)
    field

    (keyword? field)
    (DSL/field (name field))

    (vector? field)
    (DSL/array (into-array Field (columns field)))

    (map? field)
    (let [ks (keys field)]
      (if (and (= (count ks) 1) (keyword? (first ks)))
        (let [k (name (first ks))
              v (column (first (vals field)))]
          (.as ^Field v k))
        (let [pairs (into [] field)
              pairs (mapv (fn [p]
                        (.value (DSL/key (name (first p)))
                                (column (second p))))
                      pairs)]
          (DSL/jsonObject (into-array JSONEntry pairs)))))

    :else
    (DSL/val field)))

(defn columns [fields]
  (mapv column fields))

;;(pq (.as (values [1 "a"] [2 "b"])
;         "t" (into-array String ["a" "b"])))

(defn table [t]
  (cond
        (or (keyword? t) (string? t))
        (DSL/table (name t))

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