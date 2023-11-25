(ns squery-jooq.commands.update
  (:require [squery-jooq.internal.query :refer [pipeline separate-with-forms switch-select-from update-pipeline delete-pipeline]]
            [squery-jooq.state :refer [ctx]]
            [squery-jooq.internal.common :refer [table columns column get-sql]]
            squery-jooq.operators))

;;------------------------Insert------------------------------------------

(defn insert [table-name fields values]
  (let [with-header (-> @ctx (.insertInto (table (name table-name)) (columns fields)))
        add-values #(.values with-header %)]
    (loop [values values]
      (if (empty? values)
        (.execute with-header)
        (do (add-values (first values))
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