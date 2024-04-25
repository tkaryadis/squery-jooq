(ns squery-jooq.commands.update
  (:use squery-jooq.reactor-utils.functional-interfaces)
  (:require [squery-jooq.internal.query :refer [pipeline separate-with-forms switch-select-from update-pipeline delete-pipeline]]
            [squery-jooq.state :refer [ctx]]
            [squery-jooq.internal.common :refer [table columns column get-sql record-to-vec]]
            [squery-jooq.state :as state]
            squery-jooq.operators)
  (:import (org.jooq InsertValuesStep2 SelectField Table)
           (org.jooq.impl DSL)
           (reactor.core.publisher Flux)))

;;------------------------Insert------------------------------------------


(defn insert-values
  ([table-name fields values return-fields]
   (let [insert-step (-> @ctx (.insertInto (table (name table-name)) (columns fields)))
         nvalues (count values)
         ;_ (prn nvalues)
         ]
     (loop [values values]
       (if (empty? values)
         (if (empty? return-fields)
           (if @state/reactive?
             (Flux/from insert-step)
             (.execute insert-step))
           (let [insert-step (.returning ^InsertValuesStep2 insert-step (columns return-fields))]
             (if @state/reactive?
               (let [insert-step-flux (Flux/from insert-step)]
                 (if (= nvalues 1)
                   (-> insert-step-flux
                       (.map (ffn [r] [(record-to-vec return-fields r)])))
                   (-> insert-step-flux
                       (.collectList)
                       (.map (ffn [rs]
                               (mapv (partial record-to-vec return-fields) rs)))
                       (.flux))))
               (if (= nvalues 1)
                 [(record-to-vec return-fields (.fetchOne insert-step))]
                 (mapv (partial record-to-vec return-fields) (.fetch insert-step))))))
         (do (.values insert-step (first values))
             (recur (rest values)))))))
  ([table-name fields values]
   (insert-values table-name fields values [])))

(defn get-values-orders [header fields-values]
  (loop [header header
         values []]
    (if (empty? header)
      values
      (recur (rest header) (conj values (get fields-values (first header)))))))

(defn get-all-values [header fields-values]
  (loop [fields-values fields-values
         all-values []]
    (if (empty? fields-values)
      all-values
      (let [values (get-values-orders header (first fields-values))]
        (recur (rest fields-values)
               (conj all-values values))))))

(defn insert
  ([table-name header fields-values return-fields]
   (let [header (if (or (= header [:*]) (= header []))
                  header
                  (let [first-map (into [] (first fields-values))]
                    (mapv first first-map)))
         values (get-all-values header fields-values)
         return-values (insert-values table-name header values return-fields)]
     (if (empty? return-fields)
       return-values
       (mapv (fn [values]
               (zipmap return-fields values))
             return-values))))
  ([table-name header fields-values]
   (insert table-name header fields-values [])))

;;----------------------Update----------------------------------------------

(defmacro uq [utable & uforms]
  (let [uforms (update-pipeline uforms)
        uforms (concat [`(.update (table ~utable))] uforms)
        uquery (if @state/reactive?
                 (concat (list '-> '@ctx) uforms)
                 (concat (list '-> '@ctx) uforms [`(.execute)]))
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
        dquery (if @state/reactive?
                 (Flux/from (concat (list '-> '@ctx) dforms))
                 (concat (list '-> '@ctx) dforms [`(.execute)]))
        ;_ (prn "dquery" dquery)
        ]
    `(let ~squery-jooq.operators/operators-mappings
       ~dquery)))