(ns squery-jooq.operators
  (:refer-clojure :exclude [+ inc - dec * mod
                            even? odd?
                            = not= > >= < <=
                            and or not
                            if-not cond
                            into type cast boolean double int long string? nil? some? true? false? any?
                            string? int? decimal? double? boolean? number? rand
                            get get-in assoc assoc-in dissoc
                            concat conj contains? range reverse count take subvec empty?
                            fn map filter reduce
                            first second last merge max min
                            str subs re-find re-matcher re-seq replace identity
                            long-array
                            repeat])
  (:require [clojure.core :as c]
            [squery-jooq.internal.common :refer [column columns sort-arguments values-internal row-internal]]
            [squery-jooq.utils.general :refer [nested2]]
            [squery-jooq.schema :refer [schema-types]])
  (:import (org.jooq.impl DSL)
           (org.jooq Field SelectField Row1 Row2 Row3 Row6 Row5 Row4)))

;;Operators for columns

;;---------------------------Arithmetic-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn abs [col]
  (DSL/abs ^Field (column col)))

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

(defn pow [col1 n]
  (DSL/power (column col1) n))

(defn mod [col other]
  (.mod (column col) (column other)))

(defn ceil [col]
  (DSL/ceil (column col)))

(defn floor [col]
  (DSL/floor (column col)))

(defn exp [col]
  (DSL/exp (column col)))

(def e (DSL/e))

(defn max [& cols]
  (DSL/greatest (column (c/first cols)) (columns (rest cols))))

(defn min [& cols]
  (DSL/least (column (c/first cols)) (columns (rest cols))))

;;---------------------------Comparison-------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


;;TODO row instead of column like    row(BOOK.AUTHOR_ID, BOOK.TITLE).eq(1, "Animal Farm");

(defn = [col1 col2]
  (.eq  (column col1) (column col2)))

#_(defn =safe [col1 col2]
  (.eqNullSafe (column col1) (column col2)))

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

#_(defn <> [col col1-left col2-right]
  (.between (column col) (column col1-left) (column col2-right)))



;;---------------------------Boolean----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn and [& cols]
  (nested2 #(.and (column %1) (column %2))  (if (c/= (c/count cols) 1)
                                              (c/concat cols [(column true)])
                                              cols)))

(defn or [& cols]
  (nested2 #(.or (column %1) (column %2)) (if (c/= (c/count cols) 1)
                                            (c/concat cols [(column false)])
                                            cols)))

(defn not [col]
  (.not (column col)))

;;---------------------------Conditional------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------


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

(defn date
    ([col] (.cast (column col) (c/get schema-types :date)))
    #_([col string-format] (functions/to_date (column col) string-format)))

(defn timestamp
  ([col] (.cast (column col) (c/get schema-types :timestamp)))
  #_([col string-format] (functions/to_timestamp (column col) string-format))
  )

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

;;---------------------------Accumulators-----------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

(defn sum [col]
  (DSL/sum (column col)))

(defn product [col]
  (DSL/product (column col)))

(defn sum-distinct [col]
  (DSL/sumDistinct (column col)))

(defn avg [col]
  (DSL/avg (column col)))

(defn max [col]
  (DSL/max (column col)))

(defn min [col]
  (DSL/min (column col)))

(defn median [col]
  (DSL/median (column col)))

(defn count-acc
  "no argument => include nulls"
  ([] (DSL/count))
  ([col] (DSL/count (column col))))

(defn str-each
  ([col seperator-str] (DSL/groupConcat (column col) seperator-str))
  ([col] (DSL/groupConcat (column col))))

(defn and-acc [col-or-cond]
  (DSL/boolAnd (column col-or-cond)))

(defn or-acc [col-or-cond]
  (DSL/boolOr (column col-or-cond)))

(defn rand-acc [col]
  (DSL/anyValue (column col)))

(defn dense-rank
  ([] (DSL/denseRank))
  ([& cols] (DSL/denseRank (into-array Field (columns cols)))))

(defn rank
  ([] (DSL/rank))
  ([& cols] (DSL/rank (into-array Field (columns cols)))))

(defn mode
  ([] (DSL/mode))
  ([col] (DSL/mode (column col))))

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

(defn sort-group [sort-vec col]
  (.withinGroupOrderBy (column col) (sort-arguments sort-vec)))

;;------------------------------------JSON-Arrays---------------------------

(defn conj-each [col]
  (DSL/arrayAgg (column col)))

(defn conj-each-distinct [col]
  (DSL/arrayAggDistinct (column col)))

(defn conj-each-j [col]
  (DSL/jsonbArrayAgg (column col)))

(defn conj-each-jb [col]
  (DSL/jsonbArrayAgg (column col)))

(defn merge-acc [col]
  (DSL/jsonbObjectAgg (column col)))

(defn multiset [& cols]
  (DSL/multisetAgg (into-array Field (columns cols))))

(defn get [col-array idx]
  (DSL/arrayGet (column col-array) (if (c/number? idx)
                                     (c/inc (c/int idx))
                                     (inc (column idx)))))

(defn get-in-doc
  ([doc path-vec]
   (let [path-str (c/reduce (c/fn [v t]
                              (if (c/number? t)
                                (c/str v "[" t "]")
                                (c/str v "." t)))
                            "$"
                            path-vec)]
     (DSL/jsonbValue (column doc) path-str)))
  ([doc path-vec cast-type]
   (let [jvalue (get-in-doc doc path-vec)]
     (cast jvalue cast-type))))


(defn array [& cols]
  (DSL/array (into-array Field (columns cols))))


;;---------------------------Strings----------------------------------------
;;--------------------------------------------------------------------------
;;--------------------------------------------------------------------------

#_(defn re-find? [match-regex-string col]
  (.rlike (column col) match-regex-string))

#_(defn re-find
  ([match-regex-string col] (functions/regexp_extract (column col) match-regex-string (c/int 0)))
  ([match-regex-string col grou-idx-number] (functions/regexp_extract (column col) match-regex-string (c/int grou-idx-number))))

#_(defn concat
  "works on strings, binary and arrays"
  [& cols]
  (DSL/concat (into-array Field (columns cols))))

(defn str
  "concat just for strings"
  [& cols]
  (DSL/concat (into-array Field (columns cols))))

;;array_join(Column column, String delimiter)
#_(defn join-str
  ([delimiter-string col] (functions/array_join (column col) delimiter-string))
  ([col] (functions/array_join (column col) "")))

(defn count-str [col]
  (DSL/length (column col)))

(defn take-str
  ([start-int len-int col] (DSL/substring (column col) start-int len-int))
  ([start-int col] (DSL/substring (column col) start-int Integer/MAX_VALUE)))


#_(defn replace [col match-col-or-string replacement-col-or-string]
  (if (c/and (c/string? match-col-or-string) (c/string? replacement-col-or-string))
    (functions/regexp_replace (column col) match-col-or-string replacement-col-or-string)
    (functions/regexp_replace (column col) (column match-col-or-string) (column replacement-col-or-string))))

#_(defn split-str
  ([col pattern-string] (functions/split (column col) pattern-string))
  ([col pattern-string limit-int] (functions/split (column col) pattern-string limit-int)))

#_(defn substring? [str col]
  (.contains ^Column (column col) str))

#_(defn capitalize [col]
  (DSL/initcap (column col)))

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


(defn translate
  "replaces characters(no need for full match), based on index,
   index 2 of string-match with index 2 of string-replacement"
  [col string-match string-replacement]
  (DSL/translate (column col) string-match string-replacement))

(defn repeat [col ntimes]
  (DSL/repeat (column col) (c/int ntimes)))

(defn like [pattern-str col]
  (.like (column col) pattern-str))

(defn not-like [pattern-str col]
  (.notLike (column col) pattern-str))

;;-----------------------------------subqueries-----------------------------

(defn exists? [query]
  (DSL/exists query))

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

;;TODO no need to override clojure, i can have internal names with other names
;;extra cost is minimal but maybe i can use a walk in the macro to see which operators the query needs only
(def operators-mappings
  '[
    ;;Arithmetic
    abs squery-jooq.operators/abs
    +   squery-jooq.operators/+
    inc squery-jooq.operators/inc
    -   squery-jooq.operators/-
    dec squery-jooq.operators/dec
    *   squery-jooq.operators/*
    mod squery-jooq.operators/mod
    ceil  squery-jooq.operators/ceil
    floor squery-jooq.operators/floor
    exp   squery-jooq.operators/exp
    e     squery-jooq.operators/e

    ;;Comparison
    =   squery-jooq.operators/=
    not=   squery-jooq.operators/not=
    >   squery-jooq.operators/>
    >=   squery-jooq.operators/>=
    <   squery-jooq.operators/<
    <=   squery-jooq.operators/<=


    ;;Booleans
    and squery-jooq.operators/and
    or  squery-jooq.operators/or
    not squery-jooq.operators/not

    ;;Conditional
    if- squery-jooq.operators/if-
    cond squery-jooq.operators/cond

    ;;types and covert
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
    date   squery-jooq.operators/date
    timestamp  squery-jooq.operators/timestamp

    ;;strings
    str squery-jooq.operators/str
    count-str squery-jooq.operators/count-str
    take-str  squery-jooq.operators/take-str
    lower-case squery-jooq.operators/lower-case
    upper-case squery-jooq.operators/upper-case
    =ignore-case squery-jooq.operators/=ignore-case
    trim squery-jooq.operators/trim
    triml  squery-jooq.operators/triml
    trimr  squery-jooq.operators/trimr
    padl   squery-jooq.operators/padl
    padr   squery-jooq.operators/padr
    translate  squery-jooq.operators/translate
    repeat     squery-jooq.operators/repeat
    like       squery-jooq.operators/like
    not-like   squery-jooq.operators/not-like


    ;;accumulators
    sum squery-jooq.operators/sum
    product squery-jooq.operators/product
    sum-distinct squery-jooq.operators/sum-distinct
    avg squery-jooq.operators/avg
    min squery-jooq.operators/min
    max squery-jooq.operators/max
    median squery-jooq.operators/median
    count-acc  squery-jooq.operators/count-acc
    str-each  squery-jooq.operators/str-each
    rand-acc squery-jooq.operators/rand-acc
    mode squery-jooq.operators/mode
    multiset squery-jooq.operators/multiset
    percent-rank squery-jooq.operators/percent-rank

    ;;accumulators-options
    count-distinct squery-jooq.operators/count-distinct
    str-each-distinct squery-jooq.operators/str-each-distinct
    filter-acc squery-jooq.operators/filter-acc
    sort-acc squery-jooq.operators/sort-acc
    sort-group  squery-jooq.operators/sort-group

    ;;json-arrays
    conj-each squery-jooq.operators/conj-each
    conj-each-distinct squery-jooq.operators/conj-each-distinct
    conj-each-j squery-jooq.operators/conj-each-j
    conj-each-jb squery-jooq.operators/conj-each-jb
    merge-acc squery-jooq.operators/merge-acc
    get  squery-jooq.operators/get
    array  squery-jooq.operators/array

    ;;various
    inline squery-jooq.operators/inline
    asc    squery-jooq.operators/asc
    desc   squery-jooq.operators/desc
    star   squery-jooq.operators/star
    row    squery-jooq.operators/row
    values squery-jooq.operators/values

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