(ns squery-jooq.commands.admin
  (:require [squery-jooq.internal.query :refer [pipeline separate-with-forms switch-select-from update-pipeline delete-pipeline]]
            [squery-jooq.state :refer [ctx]]
            [squery-jooq.schema :refer [schema-types]]
            [squery-jooq.internal.common :refer [table columns column get-sql
                                                 squery-vector->squery-map]]
            [squery-jooq.internal.admin :refer [add-table-columns add-table-constraints]]
            [squery-jooq.utils.general :refer [ordered-map]]
            squery-jooq.operators)
  (:import (org.jooq Context DSLContext)
           (org.jooq.impl DSL)))

;;---------------------------create----

(defn create-database [db-name]
  (-> @ctx
      (.createDatabase db-name)
      (.execute)))

;;column names, types, default values, constrains

;;// Create a domain on a base type
;create.createDomain("d1").as(INTEGER).execute();
;// Create a domain on a base type and add a DEFAULT expression
;create.createDomain("d2").as(INTEGER).default_(1).execute();
;// Create a domain on a base type and add a CHECK constraint
;create.createDomain("d3").as(INTEGER).constraints(check(value(INTEGER).gt(0))).execute();

#_(defn create-domain [name type default-value cont])

;;--------------------------create-function(not free on jooq)---------------------

#_(defn add-parametes-and-types [f-obj f-args]
  (if (empty? f-args)
    f-obj
    (loop [f-args (map vec (partition 2 f-args))
           f-args-donain []]
      (if (empty? f-args)
        (let [p-f #(.parameters )])
        (let [[arg-type arg-name] (first f-args)
              arg-type (get schema-types arg-type arg-type)]
          (recur (rest f-args)
                 (conj f-args-donain (DSL/in (str arg-name) arg-type))))))))

;;scalar functions
#_(defmacro defnsql [f-name return-type f-args f-body]
  (-> ^DSLContext @ctx
      (.createFunction f-name)
      (.returns return-type)
      (.execute)))

#_(-> ^DSLContext @ctx
    (.createFunction "test")
    (.returns Integer)
    (.as (DSL/return_ 1))
    (.execute))

;;-----------------------------------create-index--------------------------------

;;(create-indexes :rejoy.users (index [:userid]))

(defn get-index-name [index-sorted-map]
  (clojure.string/join "_" (map (fn [m]
                                  (if (keyword? m)
                                    (name m)
                                    m))
                                (flatten (into [] index-sorted-map)))))

(defn index
  "Index definition,one create-index command can take one or many indexes
  Each index will be a member of :indexes [],see create-index command
  keys-vec = [:field1 :!field2 [:field3 1] [:field4 -1]...]
  keys-vec = [[:field1 'text']]  ; for text index
  Index name if not given = 'fieldName_type_fieldName_type'
  Call
  (index [:field1 :!field2 ..]) "
  [keys-vec & args]
  (let [index-sorted-map (squery-vector->squery-map (mapv (fn [k]
                                                            (if (vector? k) (into {} [k]) k))
                                                          keys-vec)
                                                    -1)
        index-name (get-index-name index-sorted-map)
        options-map (apply (partial merge {})args)
        options-map (if (contains? options-map :name)
                      options-map
                      (assoc options-map :name index-name))]
    {:index (merge {:key index-sorted-map} options-map)}))

(defn create-indexes
  "Creates one or more indexes
  Index name if not given = 'fieldName_type_fieldName_type'
  Call
  (create-indexes (index keys-vec option1 ...) (index ...)  option1)"
  [table-name & args]
  (let [[indexes options] (reduce (fn [[indexes options] m]
                                    (if (contains? m :index)
                                      [(conj indexes (get m :index)) options]
                                      [indexes (conj options m)]))
                                  [[] []]
                                  args)
        options-map (apply (partial merge {}) options)

        ]
    (loop [indexes indexes]
      (if-not (empty? indexes)
        (let [idx (first indexes)
              idx-name (get idx :name)
              unique? (get idx :unique)
              idx-columns (into [] (get idx :key))
              idx-ordered-fields (mapv
                                   (fn [idx]
                                     (if (= (second idx) -1)
                                       (.desc (column (first idx)))
                                       (.asc (column (first idx)))))
                                   idx-columns)
              f-create (fn [c]
                         (if unique?
                           (.createUniqueIndex c idx-name)
                           (.createIndex c idx-name)))]
          (recur (do (-> ^DSLContext @ctx
                         f-create
                         (.on (DSL/table (name table-name))
                              idx-ordered-fields)
                         (.execute))
                     (rest indexes))))))))

(defn create-index [coll-namespace index & options]
  (apply (partial create-indexes coll-namespace index) options))


(defn create-schema [schema-name]
  (.execute (.createSchema @ctx schema-name)))

(defn create-table [table-name cols-or-select & constraints-or-data?]
  "cols is the schema nested vectors
   [:col-name  ;;assumes type string and default nil or not
    ;;type normally a keyword
    ;;the 3rd is optional for nullable or not
    ;;if i need more complicated things, instead of keyword-type
    ;;i give an object like
    ;;   INTEGER.default_(1)   for default value
    ;;   INTEGER.identity(true) for identity like AUTOINCREMENT,serial
    ;;   VARCHAR.generatedAlwaysAs(field(name('interest'), DOUBLE).times(100.0).concat('%'))
    ;;;  ^ for computed columns
    [:col-name :type-or-Object :true-or-false-or-optional]]

    constraints
    many types like
       primary key
       foreign key
       unique column(for candidate keys)
       check for value and type, for example integer >0

    Call examples(cols)
    (create-table
                :mytable
                [:col1
                 [:col2 :integer false]]
                [(DSL/primaryKey (into-array String ['col1']))])
    (create-table
              :mytable

              [:col
               ;;3rd arg is for nil?
               [:col :type-or-Object :true-or-false-or-optional]]

              [{:primary-key ['col1' 'col2']}      ;;clojure to avoid raw jooq(not yet done)
               {:unique [['col1' 'col2']]}

               ;;if not map, i can use raw JOOQ
               ;(DSL/primaryKey (into-array String ['col1' 'col2']))
               #_(.references (DSL/foreignKey 'col1')
                            'other-table'
                            'other-column')
               ;(DSL/unique (into-array String ['col1' 'col2']))
               #_(DSL/check (.gt (DSL/field (DSL/name 'col1')
                                          SQLDataType/INTEGER)
                               0))
               ;;constraints can have names also
               #_(.primaryKey ^ConstraintTypeStep (DSL/constraint 'myConstraint')
                            (into-array String ['col1']))

               #_(.foreignKey ^ConstraintTypeStep (DSL/constraint 'myConstraint')
                            (into-array String ['col1']))

               ;;constraint('fk').foreignKey('column1').references('other_table', 'other_column1')
               ])

      Call examples(select)
      (create-table :mytable (q ...) true)    true is the default to copy the data also
      (create-table :mytable (q ...) false)
      "
  (if (vector? cols-or-select)
    (-> ^DSLContext @ctx
        (.createTable (name table-name))
        (add-table-columns cols-or-select)
        (add-table-constraints constraints-or-data?)
        ;(.getSQL)
        (.execute)
        )
    (let [table (-> ^DSLContext @ctx
                    (.createTable (name table-name))
                    (.as cols-or-select))
          table (if (false? constraints-or-data?)
                  (.withNoData table)
                  table)]
      (.execute table))))

(defn create-temp-table [table-name cols-or-select & constraints-or-data?]
  (if (vector? cols-or-select)
    (-> ^DSLContext @ctx
        (.createTemporaryTable (name table-name))
        (add-table-columns cols-or-select)
        (add-table-constraints constraints-or-data?)
        ;(.getSQL)
        (.execute)
        )
    (let [table (-> ^DSLContext @ctx
                    (.createTemporaryTable (name table-name))
                    (.as cols-or-select))
          table (if (false? constraints-or-data?)
                  (.withNoData table)
                  table)]
      (.execute table))))

(defn drop-database [db-name]
  (-> ^DSLContext @ctx
      (.dropDatabase (name db-name))
      (.execute)))

(defn drop-database-if-exists [db-name]
  (-> ^DSLContext @ctx
      (.dropDatabaseIfExists (name db-name))
      (.execute)))

(defn drop-index
  ([index-name options]
   (cond
     (true? (get options :cascade))
     (-> ^DSLContext @ctx
         (.dropIndex (name index-name))
         (.cascade)
         (.execute))

     (true? (get options :restrict))
     (-> ^DSLContext @ctx
         (.dropIndex (name index-name))
         (.restrict)
         (.execute))

     :else
     (-> ^DSLContext @ctx
         (.dropIndex (name index-name))
         (.execute))))
  ([index-name]
   (drop-index index-name)))

(defn drop-index-if-exists
  ([index-name options]
   (cond
     (true? (get options :cascade))
     (-> ^DSLContext @ctx
         (.dropIndexIfExists (name index-name))
         (.cascade)
         (.execute))

     (true? (get options :restrict))
     (-> ^DSLContext @ctx
         (.dropIndexIfExists (name index-name))
         (.restrict)
         (.execute))

     :else
     (-> ^DSLContext @ctx
         (.dropIndexIfExists (name index-name))
         (.execute))))
  ([index-name]
   (drop-index-if-exists index-name {})))

(defn drop-table
  ([table-name options]
   (cond
     (true? (get options :cascade))
     (-> ^DSLContext @ctx
         (.dropTable (name table-name))
         (.cascade)
         (.execute))

     (true? (get options :restrict))
     (-> ^DSLContext @ctx
         (.dropTable (name table-name))
         (.restrict)
         (.execute))

     :else
     (-> ^DSLContext @ctx
         (.dropTable (name table-name))
         (.execute)))
   ([table-name]
    (drop-table table-name {}))))

(defn drop-table-if-exists
  ([table-name options]
   (cond
     (true? (get options :cascade))
     (-> ^DSLContext @ctx
         (.dropTableIfExists (name table-name))
         (.cascade)
         (.execute))

     (true? (get options :restrict))
     (-> ^DSLContext @ctx
         (.dropTableIfExists (name table-name))
         (.restrict)
         (.execute))

     :else
     (-> ^DSLContext @ctx
         (.dropTableIfExists (name table-name))
         (.execute))))
  ([table-name]
   (drop-table-if-exists table-name {})))