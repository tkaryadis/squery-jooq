(ns squery-jooq.operators
  (:refer-clojure :exclude [+ inc - dec * mod
                            abs
                            even? odd?
                            = not= > >= < <=
                            and or not
                            if-not cond
                            into type cast boolean double int long string? nil? some? true? false? any?
                            string? int? decimal? double? boolean? number? rand
                            get get-in assoc assoc-in dissoc keys vals
                            aget array
                            amap
                            distinct?
                            concat conj contains? range reverse count take subvec empty?
                            fn map filter reduce
                            first second last merge max min
                            str subs re-find re-matcher re-seq replace identity
                            long-array
                            repeat])
  (:require [clojure.core :as c]
            [squery-jooq.internal.common :refer
             [column columns cond-column subquery? cond-columns sort-arguments values-internal row-internal get-field-sql]]
            [squery-jooq.utils.general :refer [nested2]]
            [squery-jooq.schema :refer [schema-types]]
            [squery-jooq.state :refer [ctx]])
  (:import (org.jooq.impl DSL Function SQLDataType)
           (org.jooq DSLContext Field GroupField JSONEntry OrderedAggregateFunction Param Select SelectField Row1 Row2 Row3 Row6 Row5 Row4 WindowFinalStep WindowSpecificationRowsStep)))

;;Operators for columns

;;TODO
;;many operators accept subqueries also, i need to check the arg type
;;  before calling, for exaple the in
;;some operators also take row value expressions, for example nil? row(....)

;;rows

(defn row [& vls]
  (apply #(DSL/row %) (c/map column vls)))


;;---------------------------Arithmetic-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn abs [col]
  (DSL/abs (column col)))

(defn neg [col]
  (DSL/neg (column col)))

;;pos=>1 neg=>-1 else 0
(defn sign [col]
  (DSL/sign (column col)))

(defn +
  "works on dates also, adds days"
  [& cols]
  (nested2 #(.add (column %1) (column %2)) cols))

(defn inc [col]
  (.add (column col) (DSL/val 1)))

(defn -
  ([col] (DSL/neg (column col)))
  ([col1 col2] (.sub (column col1) (column col2))))

(defn dec [col]
  (.sub (column col) (DSL/val 1)))

(defn * [& cols]
  (nested2 #(.mul (column %1) (column %2)) cols))

(defn div [col1 col2]
  (.div (column col1) (column col2)))

(defn pow [col1 n]
  (DSL/power (column col1) n))

(defn sqrt [col]
  (DSL/sqrt (column col)))

(defn mod [col other]
  (.mod (column col) (column other)))

(defn ceil [col]
  (DSL/ceil (column col)))

(defn floor [col]
  (DSL/floor (column col)))

(defn round
  "< .5 floor else ceil"
  [col]
  (DSL/round col))

(defn exp [col]
  (DSL/exp (column col)))

(def e (DSL/e))
(def pi (DSL/pi))

(defn ln [col]
  (DSL/ln (column col)))

(defn log [col base]
  (DSL/log (column col) base))

(defn log10 [col base]
  (DSL/log10 (column col)))

(defn max [& cols]
  (if (c/keyword? (c/first cols))
    (DSL/greatest (column (c/first cols))
                  (into-array Field (columns (rest cols))))
    (DSL/greatest (column (c/first cols))
                  (into-array Object (columns (rest cols))))))

(defn min [& cols]
  (if (c/keyword? (c/first cols))
    (DSL/least (column (c/first cols))
               (into-array Field (columns (rest cols))))
    (DSL/least (column (c/first cols))
               (into-array Object (columns (rest cols))))))

(defn random "float 0 to 1" [] (DSL/rand))

(defn bucket-width
  "divides high-low in nbuckets
   returns the bucket number where the col value falls in
   width_bucket(15, 0, 100, 10) => 2 , 15 will go in the second"
  [col low high nbuckets]
  (DSL/widthBucket (column col) low high nbuckets))

;;---------------------------Comparison-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;TODO row instead of column like    row(BOOK.AUTHOR_ID, BOOK.TITLE).eq(1, "Animal Farm");

(defn = [col1 col2]
  (.eq (column col1) (column col2)))

;;null safe =
;;[ANY] IS DISTINCT FROM NULL yields TRUE
;;[ANY] IS NOT DISTINCT FROM NULL yields FALSE
;;NULL IS DISTINCT FROM NULL yields FALSE
;;NULL IS NOT DISTINCT FROM NULL yields TRUE
(defn =safe [col1 col2]
  (.isNotDistinctFrom ^Field (column col1) (column col2)))

(defn not= [col1 col2]
  (.ne  (column col1) (column col2)))

(defn > [col1 col2]
  (.gt  (column col1) (column col2)))

(defn >= [col1 col2]
  (.ge (column col1) (column col2)))

(defn < [col1 col2]
  (.lt (column col1) (column col2) ))

(defn <= [col1 col2]
  (.le (column col1) (column col2)))

(defn <<
  "left <= col <= right"
  [col col1-left col2-right]
  (.and (.between (column col) (column col1-left)) (column col2-right)))

(defn <>
  "left <= col <= right"
  [col col1-left col2-right]
  (.and (.betweenSymmetric (column col) (column col1-left)) (column col2-right)))

(defn in [col & col-values]
  (if (subquery? col-values)
    (.in ^Field (column col) (c/first col-values))
    (.in ^Field (column col) (into-array Field (columns col-values)))))

;;---------------------------Boolean----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn and [& cols]
  (let [cols (cond-columns cols)]
    (nested2 #(DSL/and %1 %2) (if (c/= (c/count cols) 1)
                                (c/concat cols [(DSL/and (column true))])
                                cols))))

(defn or [& cols]
  (let [cols (cond-columns cols)]
    (nested2 #(DSL/or %1 %2) (if (c/= (c/count cols) 1)
                                (c/concat cols [(DSL/and (column true))])
                                cols))))

(defn xor [& cols]
  (let [cols (cond-columns cols)]
    (nested2 #(DSL/xor %1 %2) (if (c/= (c/count cols) 1)
                               (c/concat cols [(DSL/and (column true))])
                               cols))))

(defn not [col]
  (DSL/not (cond-column col)))

;;---------------------------Conditional------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;;bad idea, useless
(defn choose [idx options-vec]
  "just select from vec based on index"
  (if (c/keyword? (c/first options-vec))
    (DSL/choose (inc idx) (into-array Field (mapv column options-vec)))
    (DSL/choose (inc idx) (into-array Object options-vec))))

;;people.select(when(people("gender") === "male", 0)
;     .when(people("gender") === "female", 1)
;     .otherwise(2))

;;otherwise(Object value)
;;when(Column condition, Object value)

(defn if- [col-condition col-value col-else-value]
  (DSL/iif (column col-condition) (column col-value) (column col-else-value)))

#_(defn if-not [col-condition col-value col-else-value]
  (.otherwise (functions/when (functions/not (column col-condition)) (column col-value)) (column col-else-value)))

(defn pair-to-sql-pair [o pair]
  (c/let [f (c/first pair)
          s (c/second pair)]
    (c/cond

      (c/nil? o)
      (DSL/when (column f) (column s))

      (c/= f :else)
      (.otherwise o (column s))

      :else
      (.when o (column f) (column  s)))))

(defn cond
  "Call(like clojure)
  (cond- cond1 e1   ; cond1=boolean-e
         cond2 e2
         ...
         :else en)"
  [& args]
  (let [args (partition 2 args)
        first-pair (c/first args)
        rest-pairs (c/rest args)]
    (c/reduce (c/fn [o pair]
                (pair-to-sql-pair o pair))
              (pair-to-sql-pair nil first-pair)
              rest-pairs)))



;;---------------------------Literal----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn lit [v]
  (DSL/val v))

;;---------------------------Types and Convert------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn type [col]
  (.getTypeName (.getDataType (column col))))

;;type predicates
(defn true? [col]
  (.eq (column col) true))

(defn false? [col]
  (.eq (column col) false))

(defn nil?
  "(= :field nil) Doesn't work use this only for nil"
  [col]
  (.isNull (column col)))

(defn some? [col]
  (.isNotNull (column col)))

(defn if-nil? [col nil-value-col]
  (if- (nil? col) (column nil-value-col) (column col)))

(defn coalesce
  "returns the first not nil value"
  [& cols]
  (DSL/coalesce (column (c/first cols)) (into-array Field (columns (rest cols)))))

;;convert
(defn cast [col to-type]
  (.cast (column col) (c/cond
                        (c/string? to-type)
                        (c/get schema-types (c/keyword to-type))

                        (c/keyword? to-type)
                        (c/get schema-types to-type)

                        :else
                        to-type)))

(defn string [col]
  (.cast (column col) (c/get schema-types :string)))

(defn long [col]
  (.cast (column col) (c/get schema-types :long)))

(defn int [col]
  (.cast (column col) (c/get schema-types :int)))

(defn double [col]
  (.cast (column col) (c/get schema-types :double)))

(defn date
    ([col] (.cast (column col) (c/get schema-types :date)))
    #_([col string-format] (functions/to_date (column col) string-format)))

(defn timestamp
  ([col] (.cast (column col) (c/get schema-types :timestamp)))
  #_([col string-format] (functions/to_timestamp (column col) string-format))
  )

(defn string? [col]
  (.equals (.getDataType (column col)) SQLDataType/VARCHAR))

(defn jsonb? [col]
  (.equals (.getDataType (column col)) SQLDataType/JSONB))

(defn nil? [col]
  (.isNull (column col)))

#_(defn long-array
  ([col] (cast (column col) (array-type :long)))
  ([] (cast (column []) (array-type :long))))

#_(defn string-array
  ([col] (cast (column col) (array-type :string)))
  ([] (cast (column []) (array-type :string))))

#_(defn date-array
  ([col] (cast (column col) (array-type :date)))
  ([] (cast (column []) (array-type :date))))

#_(defn col [c]
  (functions/col (if (keyword? c) (name c) c)))

#_(defn ->col [c]
  (squery-spark.datasets.internal.common/column c))


#_(defn format-number [col d]
  (functions/format_number (column col) d))

#_(defn array [& cols]
  (functions/array (into-array Column (columns cols))))

#_(defn json [col builded-schema]
  (functions/from_json (column col) builded-schema))

;;----------------------------dates-----------------------------------------

(defn current-date []
  (DSL/currentDate))

;;TODO page 530
#_(defn overlap? [row-date-range1 row-date-range2]
  (.overlaps ^Row2 row-date-range1 row-date-range2))

;;---------------------------Accumulators-----------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;;skipped
;;  all BIT acc
;;  CUME_DIST
;;  XMLAGG

(defn sum-acc [col]
  (DSL/sum (column col)))

(defn product-acc [col]
  (DSL/product (column col)))

(defn product-acc [col]
  (DSL/product (column col)))

(defn sum-distinct-acc [col]
  (DSL/sumDistinct (column col)))

(defn avg-acc [col]
  (DSL/avg (column col)))

(defn max-acc [col]
  (DSL/max (column col)))

(defn min-acc [col]
  (DSL/min (column col)))

(defn median-acc [col]
  (DSL/median (column col)))

(defn count-acc
  "no argument => include nulls"
  ([] (DSL/count))
  ([col] (DSL/count (column col))))

;;there is also the LISTAGG, but looks the same so didnt added
(defn str-each
  ([col seperator-str] (DSL/groupConcat (column col) seperator-str))
  ([col] (DSL/groupConcat (column col))))

(defn and-acc [col-or-cond]
  (DSL/boolAnd (column col-or-cond)))

(defn or-acc [col-or-cond]
  (DSL/boolOr (column col-or-cond)))

(defn rand-acc [col]
  (DSL/anyValue (column col)))

(defn mode
  ([] (DSL/mode))
  ([col] (DSL/mode (column col))))

(defn multiset-acc [& cols]
  (DSL/multisetAgg (into-array Field (columns cols))))


;;----------------hypothetical set functions accumulators----------

;;i use those with sort-group  (within group)

;;skipped PERCENTILE_CONT,PERCENTILE_DISC

(defn cume-dist [& cols]
  (DSL/cumeDist (into-array Field (columns cols))))

;;equals same rank
(defn dense-rank
  ([] (DSL/denseRank))
  ([& cols] (DSL/denseRank (into-array Field (columns cols)))))

;;equal not same rank
(defn rank
  ([] (DSL/rank))
  ([& cols] (DSL/rank (into-array Field (columns cols)))))

;;percentRank(val(0)).withinGroupOrderBy(BOOK.ID)
(defn percent-rank
  ([] (DSL/percentRank))
  ([cols] (DSL/percentRank (into-array Field (columns cols)))))

;;-------------------------------accumulators-options-----------------------

(defn count-distinct
  ([& cols] (DSL/countDistinct (into-array Field (columns cols)))))

(defn str-each-distinct [col]
  (DSL/groupConcatDistinct (column col)))

(defn filter-acc [f col]
  (.filterWhere (column col) f))

(defn sort-acc [sort-vec col]
  (.orderBy (column col) (sort-arguments sort-vec)))

(defn sort-group [sort-vec set-function]
  (.withinGroupOrderBy ^OrderedAggregateFunction set-function
                       (sort-arguments sort-vec)))

;;------------------------------array-accumulators-------------------------

;;no-mysql,postgress-ok
(defn aconj-each[col]
    (DSL/arrayAgg (column col)))

;;no-mysql,postgress-ok
(defn aconj-each-distinct [col]
    (DSL/arrayAggDistinct (column col)))

;;------------------------------json-accumulators--------------------------

;;not working on mysql, postgres is ok
(defn conj-each [col]
  (DSL/jsonbArrayAgg (column col)))

;;mysql+postgress ok
(defn merge-acc [k v]
  (DSL/jsonbObjectAgg (column k) (column v)))

;;----------------------------window-functions------------------------------

(defn ws-sort [sort-vec]
  (DSL/orderBy (into-array (sort-arguments sort-vec))))

(defn ws-group [& cols]
  (DSL/partitionBy (into-array Field (columns cols))))

(defn window
  ([acc-fun window-spec] (.over acc-fun window-spec))
  ([acc-fun] (.over (column acc-fun))))

;;--------------------------window-accumulators------------------------------

(defn row-number []
  (DSL/rowNumber))

;;--------------------------window-frame-limit-------------------------------

(defn rows-preceding [wind nrows]
  (.rowsPreceding wind (c/int nrows)))

(defn range-preceding [wind nrows]
  (.rangePreceding wind (c/int nrows)))

(defn groups-preceding [wind nrows]
  (.groupsPreceding wind (c/int nrows)))

;;-------------------------------------arrays-not-json-arrays---------------

(defn array [& cols]
  (DSL/array (into-array Field (columns cols))))

(defn qarray [query]
  (DSL/array query))

(defn acount [col-ar]
  (DSL/cardinality (column col-ar)))

(defn aconcat [& arrays]
  (nested2 #(DSL/arrayConcat (column %1) (column %2))
           arrays))

(defn aget [col-array idx]
  (DSL/arrayGet (column col-array) (if (c/number? idx)
                                     (c/inc (c/int idx))
                                     (inc (column idx)))))

;;true if common members(not empty intersection)
(defn aand [col-ar1 col-ar2]
  (DSL/arrayOverlap (column col-ar1) (column col-ar2)))

(defn aassoc [col-ar idx vl]
  (DSL/arrayReplace (column col-ar) (+ (column idx) 1) (column vl)))

(defn adissoc [col-ar idx]
  (DSL/arrayRemove (column col-ar) (+ (column idx) 1)))

;;------------------------------------JSON objects+arrays---------------------------

;;i think its best to use json-arrays only , not arrays

;;insert/remove/replace/set only for mysql? not postgress?

(defn json-array [& fields]
  (DSL/jsonbArray (into-array Field (columns fields))))

;(nested2 #(.add (column %1) (column %2)) cols)
#_(DSL/jsonbObject (name k) v)
(defn json-object [& kvs]
  (let [kvs (c/reduce  (c/fn [v t]
                         (c/conj v (.value (DSL/key (c/name (c/first t))) (c/second t))))
                       []
                       (c/partition 2 kvs))]
    (DSL/jsonbObject (into-array JSONEntry kvs))))

;;mysql ok, no postgress
#_(defn assoc-insert [col k v]
  (DSL/jsonbInsert (column col)
                  (if (c/keyword? k)
                    (c/str "$." (c/name k))
                    (c/str "$." k))
                  (column v)))

;;mysql ok, no postgress
#_(defn assoc-update [col k v]
  (DSL/jsonbReplace (column col)
                   (if (c/keyword? k)
                     (c/str "$." (c/name k))
                     (c/str "$." k))
                    (column v)))

;;mysql ok, no postgress
#_(defn assoc [col k v]
  (DSL/jsonbSet (column col)
                (if (c/keyword? k)
                  (c/str "$." (c/name k))
                  (c/str "$." k))
                (column v)))

;;mysql ok , no postgress
#_(defn dissoc [col k]
  (DSL/jsonbRemove (column col)
                   (if (c/keyword? k)
                     (c/str "$." (c/name k))
                     (c/str "$." k))))

;;TODO for vec(array), path-vec doesnt work for columns only for numbers
;;for maps columns are ok
(defn get-in
  ([doc-or-array path-vec]
   (let [path-str (c/reduce (c/fn [v t]
                              (c/cond

                                (c/number? t)
                                (c/str v "[" t "]")

                                (c/keyword? t)
                                (c/str v "." (c/name t))

                                :else
                                (c/str v "." t)))
                            "$"
                            path-vec)]
     (DSL/jsonbValue (column doc-or-array) path-str)))
  ([doc path-vec cast-type]
   (let [jvalue (get-in doc path-vec)]
     (cast jvalue cast-type))))

(defn get
  ([doc-or-array k]
   (get-in doc-or-array [k]))
  ([doc-or-array k cast-type]
   (get-in doc-or-array [k] cast-type)))

(defn get-string [doc-or-array k]
  (get doc-or-array k :string))

(defn get-long [doc-or-array k]
  (get doc-or-array k :long))

(defn get-double [doc-or-array k]
  (get doc-or-array k :double))

(defn keys [col]
  (DSL/jsonbKeys (column col)))

(defn contains? [col k]
  (not (nil? (get-in col [k]))))


;;----------------------------Quantifiers-----------------------------------

;;TODO fields and sub-query both doesn't work on postgres?

#_(defn any [sub-query]
  (DSL/any sub-query)

  #_(if (c/vector? (c/first fields-or-vec))
    (DSL/any (into-array Field (columns (c/first fields-or-vec))))
    (DSL/any (into-array Field (columns fields-or-vec)))))

#_(defn all [& fields-or-vec]
  (if (c/vector? (c/first fields-or-vec))
    (DSL/all (into-array Field (columns (c/first fields-or-vec))))
    (DSL/all (into-array Field (columns fields-or-vec)))))

;;---------------------------Strings----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

;;skipped
;;   ascii,chr,digits,left/right(they are like substring),TO_HEX,UUID

(defn str
  "concat just for strings"
  [& cols]
  (DSL/concat (into-array Field (columns cols))))

(defn count-str [col-str]
  (DSL/length (column col-str)))

(defn subs
  ([col-str col-start-index]
   (DSL/substring (column col-str)
                  (+ (int (column col-start-index)) (c/int 1))
                  (int (column Integer/MAX_VALUE))))
  ([col-str col-start-index col-end-index]
   (DSL/substring (column col-str)
                  (+ (int (column col-start-index)) (c/int 1))
                  (- (+ (int (column col-end-index)) (c/int 1))
                     (int (column col-start-index))))))

(defn lower-case [col]
  (DSL/lower (column col)))

(defn upper-case [col]
  (DSL/upper (column col)))

(defn =ignore-case [col1 col2]
  (.equalIgnoreCase (column col1) (column col2)))

(defn trim
  ([col trim-string] (DSL/trim (column col) trim-string))
  ([col] (DSL/trim (column col))))

(defn triml
  ([col trim-string] (DSL/ltrim (column col) trim-string))
  ([col] (DSL/ltrim (column col))))

(defn trimr
  ([col trim-string] (DSL/rtrim (column col) trim-string))
  ([col] (DSL/rtrim (column col))))

(defn padl
  "Result will be a string of lenght=len-int,
   if the column is smaller pad-string will be added
   on the left"
  [col len-int pad-string]
  (DSL/lpad (column col) (c/int len-int) pad-string))

(defn padr [col len-int pad-string]
  (DSL/rpad (column col) (c/int len-int) pad-string))

(defn repeat [col ntimes]
  (DSL/repeat (column col)
              (int (column ntimes))))

(defn md5 [col-str]
  (DSL/md5 (column col-str)))

(defn overlay [col-str1 col-str2 start-index]
  (DSL/overlay (column col-str1)
               (column col-str2)
               (int (+ (column start-index) 1))))

(defn sub-index
  ([col-str1 col-str2 start-index]
   (- (DSL/position (column col-str1)
                    (column col-str2)
                    (int (+ (column start-index) 1)))
      1))
  ([col-str1 col-str2]
   (sub-index col-str1 col-str2 0)))

(defn reverse [col-str]
  (DSL/reverse (column col-str)))

(defn space [nspaces]
  (DSL/space (int (column nspaces))))

(defn split-get [col-str col-split-delimiter col-part-idx]
  (DSL/splitPart (column col-str)
                 (column col-split-delimiter)
                 (int (+ (column col-part-idx) 1))))

(defn to-string [col-str col-format]
  (DSL/toChar (column col-str) (column col-format)))

(defn translate
  "replaces characters(no need for full match), based on index,
   index 2 of string-match with index 2 of string-replacement"
  [col string-match string-replacement]
  (DSL/translate (column col) string-match string-replacement))

(defn replace-all [col-str col-str-pattern col-replacement-str]
  (DSL/regexpReplaceAll (column col-str)
                        (column (if (instance? java.util.regex.Pattern col-str-pattern)
                                  (.toString col-str-pattern)
                                  col-str-pattern))
                        (column col-replacement-str)))

(defn replace [col-str col-str-pattern col-replacement-str]
  (DSL/regexpReplaceFirst (column col-str)
                          (column (if (instance? java.util.regex.Pattern col-str-pattern)
                                    (.toString col-str-pattern)
                                    col-str-pattern))
                          (column col-replacement-str)))

(defn replace-all-str [col-str col-str-match col-replacement-str]
  (DSL/replace (column col-str)
               (column col-str-match)
               (column col-replacement-str)))

(defn like
  "% means any sequence of characters, or zero"
  [pattern-str col]
  (.like (column col) pattern-str))

(defn not-like [pattern-str col]
  (.notLike (column col) pattern-str))

(defn similar
  "_ single character not zero
   % multi character or zero"
  [pattern-str col]
  (.similarTo (column col) pattern-str))

(defn collate [col collation-str]
  (.collate (column col) collation-str))

;;-----------------------------------subqueries-----------------------------

(defn exists? [query]
  (DSL/exists query))

(defn not-exists? [query]
  (DSL/notExists query))

;;not postgres?
#_(defn distinct? [& cols]
  (if (subquery? cols)
    (DSL/unique (c/first cols))
    (DSL/unique (into-array Field (columns cols)))))

;;-----------------------------------various--------------------------------

(defn inline [position]
  (DSL/inline position))

(defn asc [col]
  (.asc (column col)))

(defn desc [col]
  (.desc (column col)))

(def star (DSL/asterisk))

(defn row [& fields]
  (apply row-internal fields))

(defn values [& rows-vec]
  (values-internal rows-vec))

(defn ignore-nil [acc-call]
  (.absentOnNull acc-call))

(defn keep-nil [acc-call]
  (.nullOnNull acc-call))

;;--------------------------json-postgres-only-------------------------

;;from jooq already have
;;get,get-in,keys,contains?

(defn merge [col-json1 col-json2]
  (DSL/field (c/str (get-field-sql (column col-json1))
                    " || "
                    (get-field-sql (column col-json2)))))

(defn concat [col1-json-array col2-json-array]
  (merge col1-json-array col2-json-array))

(defn assoc [col-json k v]
  (merge col-json (DSL/jsonbObject (column k) (column v))))

(defn dissoc [col k]
  (DSL/field (c/str (get-field-sql (column col))
                    " - "
                    (get-field-sql (column k)))))

;;jsonb_array_elements_text
(defn unwind-text [col]
  (DSL/field (c/str " jsonb_array_elements_text("
                    (get-field-sql (column col))
                    ") ")))

(defn unwind [col]
  (DSL/field (c/str " jsonb_array_elements("
                    (get-field-sql (column col))
                    ") ")))

(defn unwind-array [col]
  (DSL/field (c/str " unnest("
                    (get-field-sql (column col))
                    ") ")))

;;no need jooq supports it
#_(defn unwind-array-to-table [col table-schema]
  (.as (DSL/table (c/str " unnest("
                         (get-field-sql (column col))
                         ") "))
       (name (c/first table-schema))
       (into-array String (c/map c/name (c/rest table-schema)))))

;;array to table, with 1 column the array members
;;i can use max 2 columns also, it will be the same values in all columns
(defn unwind-array-to-table [col table-schema]
  (.as (if (c/> (c/count table-schema) 2)
         (.withOrdinality (DSL/unnest (column col)))
         (DSL/unnest (column col)))
       (name (c/first table-schema))
       (into-array String (c/map c/name (c/rest table-schema)))))

(defn unwind-to-table [col table-schema]
  (.as (DSL/table (c/str " jsonb_array_elements("
                         (get-field-sql (column col))
                         ") "))
       (name (c/first table-schema))
       (into-array String (c/map c/name (c/rest table-schema)))))

(defn into [json-type col]
  (c/cond
    (c/map? json-type)
    (DSL/field (c/str " to_jsonb("
                      (get-field-sql (column col))
                      ") "))

    (c/= json-type ["sql"])
    (DSL/array (-> @ctx (.select (unwind (column col)))))

    (c/vector? json-type)
    (DSL/field (c/str " array_to_json("
                      (get-field-sql (column col))
                      ") "))
    ;;shouldnt happen
    :else
    (column col)))


;jsonb_array_length
(defn count [col]
  (DSL/field (c/str " jsonb_array_length("
                    (get-field-sql (column col))
                    ") ")))

(defn fn [args op]
  {:args args
   :op op})

;;SELECT ARRAY(SELECT x + 1 FROM unnest(ARRAY[1, 2, 3]) AS t(x));
#_(defn map [fnn-arg col]
  (let [args (c/get fnn-arg :args)
        op  (c/get fnn-arg :op)
        _ (prn "args" args)
        _ (prn "opppp" op)]
    (DSL/array (-> @ctx
                   (.select [(+ (cast (DSL/field (column (c/second args))) (c/first args)) 1)])
                   (.from [(unwind-to-table (column col) [:t :x])])))))

(defn map [fn-arg col]
  (let [args (c/get fn-arg :args)
        op  (c/get fn-arg :op)]
    (into [] (DSL/array (-> @ctx
                            (.select [op])
                            (.from [(unwind-to-table (column col) [:t (c/first args)])]))))))

(defn filter [fn-arg col]
  (let [args (c/get fn-arg :args)
        op  (c/get fn-arg :op)]
    (into [] (DSL/array (-> @ctx
                            (.select [(column (c/first args))])
                            (.where [op])
                            (.from [(unwind-to-table (column col) [:t (c/first args)])]))))))

(defn amap [fn-arg col]
  (let [args (c/get fn-arg :args)
        op  (c/get fn-arg :op)]
    (DSL/array (-> @ctx
                   (.select [op])
                   (.from [(unwind-array-to-table (column col) [:t (c/first args)])])))))

(defn afilter [fn-arg col]
  (let [args (c/get fn-arg :args)
        op  (c/get fn-arg :op)]
    (DSL/array (-> @ctx
                   (.select [(column (c/first args))])
                   (.where [op])
                   (.from [(unwind-array-to-table (column col) [:t (c/first args)])])))))

;;---------------------table-operators

(defn table-range [start end]
  (DSL/generateSeries (int (column start)) (int (column end))))

;;TODO no need to override clojure, i can have internal names with other names
;;extra cost is minimal but maybe i can use a walk in the macro to see which operators the query needs only
(def operators-mappings
  '[
    ;;Arithmetic
    abs squery-jooq.operators/abs
    neg squery-jooq.operators/neg
    sign squery-jooq.operators/sign
    +   squery-jooq.operators/+
    inc squery-jooq.operators/inc
    -   squery-jooq.operators/-
    dec squery-jooq.operators/dec
    *   squery-jooq.operators/*
    pow squery-jooq.operators/pow
    sqrt  squery-jooq.operators/sqrt
    div   squery-jooq.operators/div
    mod squery-jooq.operators/mod
    ceil  squery-jooq.operators/ceil
    floor squery-jooq.operators/floor
    round squery-jooq.operators/round
    exp   squery-jooq.operators/exp
    e     squery-jooq.operators/e
    pi    squery-jooq.operators/pi
    ln    squery-jooq.operators/ln
    log   squery-jooq.operators/log
    log10 squery-jooq.operators/log10
    max   squery-jooq.operators/max
    min   squery-jooq.operators/min
    random  squery-jooq.operators/random
    bucket-width squery-jooq.operators/bucket-width

    ;;Comparison
    =   squery-jooq.operators/=
    not=   squery-jooq.operators/not=
    >   squery-jooq.operators/>
    >=   squery-jooq.operators/>=
    <   squery-jooq.operators/<
    <=   squery-jooq.operators/<=
    <>  squery-jooq.operators/<>


    ;;Booleans
    and squery-jooq.operators/and
    or  squery-jooq.operators/or
    not squery-jooq.operators/not

    ;;Conditional
    choose squery-jooq.operators/choose
    if- squery-jooq.operators/if-
    cond squery-jooq.operators/cond

    ;;types and covert
    type squery-jooq.operators/type
    true?  squery-jooq.operators/true?
    false? squery-jooq.operators/false?
    nil?   squery-jooq.operators/nil?
    some?  squery-jooq.operators/some?
    if-nil? squery-jooq.operators/if-nil?
    coalesce squery-jooq.operators/coalesce
    cast   squery-jooq.operators/cast
    string squery-jooq.operators/string
    long   squery-jooq.operators/long
    int    squery-jooq.operators/int
    double squery-jooq.operators/double
    date   squery-jooq.operators/date
    timestamp  squery-jooq.operators/timestamp
    string?   squery-jooq.operators/string?

    ;;strings
    str squery-jooq.operators/str
    count-str squery-jooq.operators/count-str
    subs  squery-jooq.operators/subs
    lower-case squery-jooq.operators/lower-case
    upper-case squery-jooq.operators/upper-case
    =ignore-case squery-jooq.operators/=ignore-case
    trim squery-jooq.operators/trim
    triml  squery-jooq.operators/triml
    trimr  squery-jooq.operators/trimr
    padl   squery-jooq.operators/padl
    padr   squery-jooq.operators/padr
    repeat     squery-jooq.operators/repeat
    md5        squery-jooq.operators/md5
    overlay    squery-jooq.operators/overlay
    sub-index  squery-jooq.operators/sub-index
    reverse     squery-jooq.operators/reverse
    space       squery-jooq.operators/space
    split-get   squery-jooq.operators/split-get
    to-string   squery-jooq.operators/to-string
    translate  squery-jooq.operators/translate
    replace-all   squery-jooq.operators/replace-all
    replace     squery-jooq.operators/replace
    replace-all-str squery-jooq.operators/replace-all-str

    ;;subquries
    exists?  squery-jooq.operators/exists?
    not-exists? squery-jooq.operators/not-exists?
    ;distinct?  squery-jooq.operators/distinct?

    ;;accumulators
    sum-acc squery-jooq.operators/sum-acc
    product-acc squery-jooq.operators/product-acc
    sum-distinct-acc squery-jooq.operators/sum-distinct-acc
    avg-acc squery-jooq.operators/avg-acc
    min-acc squery-jooq.operators/min-acc
    max-acc squery-jooq.operators/max-acc
    median-acc squery-jooq.operators/median-acc
    count-acc  squery-jooq.operators/count-acc
    str-each  squery-jooq.operators/str-each
    rand-acc squery-jooq.operators/rand-acc
    and-acc  squery-jooq.operators/and-acc
    or-acc   squery-jooq.operators/or-acc
    mode squery-jooq.operators/mode
    multiset-acc squery-jooq.operators/multiset-acc
    percent-rank squery-jooq.operators/percent-rank

    ;;accumulators-options
    count-distinct squery-jooq.operators/count-distinct
    str-each-distinct squery-jooq.operators/str-each-distinct
    filter-acc squery-jooq.operators/filter-acc
    sort-acc squery-jooq.operators/sort-acc
    sort-group  squery-jooq.operators/sort-group

    ;;accumulators-arrays
    aconj-each squery-jooq.operators/aconj-each
    aconj-each-distinct squery-jooq.operators/aconj-each-distinct

    ;;accumulators-json
    conj-each squery-jooq.operators/conj-each
    merge-acc squery-jooq.operators/merge-acc

    ;;arrays-not-json-arrays
    array  squery-jooq.operators/array
    qarray squery-jooq.operators/qarray
    acount squery-jooq.operators/acount
    aget squery-jooq.operators/aget
    aand squery-jooq.operators/aand
    aassoc squery-jooq.operators/aassoc
    adissoc squery-jooq.operators/adissoc
    aconcat-arrays squery-jooq.operators/aconcat

    ;;json-objects and arrays
    json-array squery-jooq.operators/json-array
    json-object squery-jooq.operators/json-object
    get-in  squery-jooq.operators/get-in
    get     squery-jooq.operators/get
    get-string squery-jooq.operators/get-string
    get-long squery-jooq.operators/get-long
    get-double squery-jooq.operators/get-double
    keys squery-jooq.operators/keys
    contains? squery-jooq.operators/contains?


    ;;quantifiers
    ;any squery-jooq.operators/any
    ;all squery-jooq.operators/all

    ;;various
    inline squery-jooq.operators/inline
    asc    squery-jooq.operators/asc
    desc   squery-jooq.operators/desc
    star   squery-jooq.operators/star
    row    squery-jooq.operators/row
    values squery-jooq.operators/values

    ;;json-postgres-only
    count   squery-jooq.operators/count
    merge squery-jooq.operators/merge
    concat squery-jooq.operators/concat
    assoc  squery-jooq.operators/assoc
    dissoc  squery-jooq.operators/dissoc
    unwind  squery-jooq.operators/unwind
    unwind-array  squery-jooq.operators/unwind-array
    into    squery-jooq.operators/into
    fn    squery-jooq.operators/fn
    map    squery-jooq.operators/map
    filter    squery-jooq.operators/filter
    amap    squery-jooq.operators/amap
    afilter  squery-jooq.operators/afilter

    ;;stages
    select squery-jooq.stages/select
    from   squery-jooq.stages/from
    where  squery-jooq.stages/where
    limit  squery-jooq.stages/limit
    skip   squery-jooq.stages/skip
    sort   squery-jooq.stages/sort
    union  squery-jooq.stages/union
    union-all  squery-jooq.stages/union-all
    except     squery-jooq.stages/except
    except-all  squery-jooq.stages/except-all
    intersection  squery-jooq.stages/intersection

    ;;stages joins
    join   squery-jooq.stages/join
    left-outer-join  squery-jooq.stages/left-outer-join
    right-outer-join  squery-jooq.stages/right-outer-join
    full-join squery-jooq.stages/full-join
    left-semi-join squery-jooq.stages/left-semi-join
    left-anti-join squery-jooq.stages/left-anti-join
    cross-join squery-jooq.stages/cross-join

    ;;update-stages
    set-columns  squery-jooq.stages/set-columns

    ])