(ns squery-jooq.commands.update
  (:require [squery-jooq.internal.query :refer [pipeline separate-with-forms switch-select-from update-pipeline delete-pipeline]]
            [squery-jooq.state :refer [ctx]]
            [squery-jooq.internal.common :refer [table columns column get-sql record-to-vec]]
            squery-jooq.operators)
  (:import (org.jooq InsertValuesStep2 SelectField Table)
           (org.jooq.impl DSL)))

;;------------------------Insert------------------------------------------


(defn insert
  ([table-name fields values return-fields]
   (let [insert-step (-> @ctx (.insertInto (table (name table-name)) (columns fields)))
         nvalues (count values)]
     (loop [values values]
       (if (empty? values)
         (if (empty? return-fields)
           (.execute insert-step)
           (let [insert-step (.returning ^InsertValuesStep2 insert-step (columns return-fields))]
             (if (= nvalues 1)
               (record-to-vec return-fields (.fetchOne insert-step))
               (mapv (partial record-to-vec return-fields) (.fetch insert-step)))))
         (do (.values insert-step (first values))
             (recur (rest values)))))))
  ([table-name fields values]
   (insert table-name fields values [])))

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