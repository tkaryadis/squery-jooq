(ns squery-jooq.commands
  (:require [squery-jooq.internal.query :refer [pipeline switch-select-from update-pipeline delete-pipeline]]
            [squery-jooq.state :refer [ctx]]
            [squery-jooq.internal.common :refer [table columns column]]
            squery-jooq.operators))

;;---------------Query macros----------------------------------------------------------------

;;q
;; 1)translates to ->
;; 2)macro is used to convert [] to project, {} to add fields, () to fitlers etc
;;   and to create the enviroment overriding some of clojure.core, only inside the q scope
;;   to access clojure.core inside the q scope use alias like c/str
;; 3)functions is used for everything else that can run on runtime
;;TODO possible bug if this enviroment is moved with a macro to another place for example -> does it
;; solution to avoid possible bug? create a custom version of -> that the enviroment is not moved at macro-expand?
(defmacro q [& qforms]
  (let [qforms (switch-select-from qforms true)
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)
        ;_ (prn "query" query)
        ]
    `(let ~squery-jooq.operators/operators-mappings
       ~query)))

(defmacro s [& qforms]
  (let [qforms (switch-select-from qforms false)
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)
        ;_ (prn "query" query)
        ]
    `(let ~squery-jooq.operators/operators-mappings
       ~query)))

(defmacro ps [& qforms]
  (let [qforms (switch-select-from qforms false)
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)
        ;_ (prn "query" query)
        ]
    `(let ~squery-jooq.operators/operators-mappings
       (squery-jooq.printing/print-json-results ~query))))

(defmacro pq [& qforms]
  (let [qforms (switch-select-from qforms true)
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)
        ;_ (prn "query" query)
        ]
    `(let ~squery-jooq.operators/operators-mappings
       (squery-jooq.printing/print-json-results ~query))))

#_(defmacro sq [arg]
    `(let ~squery-spark.datasets.operators/operators-mappings
       ~arg))

#_(defmacro sq-> [& args]
    `(let ~squery-spark.datasets.operators/operators-mappings
       (-> ~@args)))

#_(defmacro not-sq [arg]
    `(let ~squery-spark.datasets.operators/core-operators-mappings
       ~arg))

;;------------------------Insert------------------------------------------

(defn insert [table-name fields values]
  (let [with-header (-> @ctx (.insertInto (table (name table-name)) (columns fields)))]
    (loop [values values]
      (if (empty? values)
        (.execute with-header)
        (do (let [f #(.values with-header %)]
              (f (first values)))
            (recur (rest values)))))))

;;----------------------Update----------------------------------------------

(defmacro uq [utable & uforms]
  (let [uforms (update-pipeline uforms)
        uforms (concat [`(.update (table ~utable))] uforms)
        uquery (concat (list '-> '@ctx) uforms [`(.execute)])
        ;_ (prn "uquery" uquery)
        ]
    `(let ~squery-jooq.operators/operators-mappings
       ~uquery)))

;;--------------------Delete-------------------------------------------------

;;create.delete(AUTHOR)
;.where(AUTHOR.ID.eq(100))
;.execute();

(defmacro dq [dtable & dforms]
  (let [dforms (delete-pipeline dforms)
        dforms (concat [`(.delete (table ~dtable))] dforms)
        dquery (concat (list '-> '@ctx) dforms [`(.execute)])
        ;_ (prn "dquery" dquery)
        ]
    `(let ~squery-jooq.operators/operators-mappings
       ~dquery)))