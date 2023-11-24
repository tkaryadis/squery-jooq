(ns squery-jooq.commands
  (:require [squery-jooq.internal.query :refer [pipeline separate-with-forms switch-select-from update-pipeline delete-pipeline]]
            [squery-jooq.state :refer [ctx]]
            [squery-jooq.internal.common :refer [table columns column get-sql]]
            squery-jooq.operators))

;;---------------Query macros----------------------------------------------------------------

;;q (requires a from, as first argument(just a keyword table name), project last or missing(missing= keep all))
;; 1)translates to ->
;; 2)macro is used to convert [] to project, {} to add fields, () to fitlers etc
;;   and to create the enviroment overriding some of clojure.core, only inside the q scope
;;   to access clojure.core inside the q scope use alias like c/str
;; 3)functions is used for everything else that can run on runtime

;;q with(optional)
;;  table name (required)
;;  project (optional, not optional if group?)
;;  table-alias (optional, used to store result in table name)
;;returns a org.jooq.impl.SelectImpl
(defmacro q [& qforms]
  (let [[qforms with-qforms] (separate-with-forms qforms)
        qforms (switch-select-from qforms true false)
        qforms (doall (concat with-qforms qforms))
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)
        ;_ (prn "query" query)
        ;_ (prn "sql" (squery-jooq.internal.common/get-sql query))
        ]
    `(let ~squery-jooq.operators/operators-mappings
       ~query)))

(defmacro sq [& qforms]
  (let [[qforms with-qforms] (separate-with-forms qforms)
        qforms (switch-select-from qforms true true)
        qforms (doall (concat with-qforms qforms))
        qforms (pipeline qforms)
        query (concat (list '->) qforms)
        ;_ (prn "query" query)
        ]
    query))

;;s (without from)
;;TODO see 4.10.15. The DUAL table, how jooq does select without from
(defmacro s [& qforms]
  (let [[qforms with-qforms] (separate-with-forms qforms)
        qforms (switch-select-from qforms false false)
        qforms (doall (concat with-qforms qforms))
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)
        ;_ (prn "query" query)
        ;_ (prn "sql" (squery-jooq.internal.common/get-sql query))
        ]
    `(let ~squery-jooq.operators/operators-mappings
       ~query)))

(defmacro ss [& qforms]
  (let [[qforms with-qforms] (separate-with-forms qforms)
        qforms (switch-select-from qforms false true)
        qforms (doall (concat with-qforms qforms))
        qforms (pipeline qforms)
        query (concat (list '->) qforms)
        _ (println "query" query)
        ;_ (prn "sql" (squery-jooq.internal.common/get-sql query))
        ]
    query))

(defmacro ps [& qforms]
  (let [[qforms with-qforms] (separate-with-forms qforms)
        qforms (switch-select-from qforms false false)
        qforms (doall (concat with-qforms qforms))
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)
        ;_ (prn "query" query)
        ]
    `(let ~squery-jooq.operators/operators-mappings
       (prn "query" ~query)
       (println "sql" (squery-jooq.internal.common/get-sql ~query))
       (squery-jooq.printing/print-results ~query))))

(defmacro pq [& qforms]
  (let [[qforms with-qforms] (separate-with-forms qforms)
        qforms (switch-select-from qforms true false)
        qforms (doall (concat with-qforms qforms))
        qforms (pipeline qforms)
        query (concat (list '-> '@ctx) qforms)]
    `(let ~squery-jooq.operators/operators-mappings
       (prn "query" ~query)
       (println "sql" (squery-jooq.internal.common/get-sql ~query))
       (squery-jooq.printing/print-results ~query))))



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
        _ (prn "dquery" dquery)
        ]
    `(let ~squery-jooq.operators/operators-mappings
       ~dquery)))