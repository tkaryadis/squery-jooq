(ns squery-jooq.internal.query
  (:require                                                 ;squery-jooq.stages
            [squery-jooq.internal.common :refer [columns]])
  (:import (org.jooq.impl DSL)))

(defn separate-with-forms [qforms]
  (loop [qforms qforms
         new-qforms []
         with-qforms []]
    (if (empty? qforms)
      [new-qforms with-qforms]
      (let [qform (first qforms)]
        (if (and (list? qform) (= (first qform) 'with))
          (recur (rest qforms) new-qforms (conj with-qforms qform))
          (recur (rest qforms) (conj new-qforms qform) with-qforms))))))

;;assumes this order
;; from
;; ...
;; select (optional)
;; alias (optional)
(defn switch-select-from [qforms from-also? nested?]
  (let [;;first always the from
        qforms (if from-also?
                 (let [from (first qforms)]
                   (if (and (list? from) (= (first from) 'from))
                     qforms
                     (let [
                           ;;used in case of values instead of table name
                           ;;.from(values(row(1, "a"),row(2, "b")).as("t", "a", "b"))
                           ;;(pq [[:t :d :c]  [1 "a"] [2 "b"]])
                           from (if (and (vector? from) (vector? (first from)))
                                  (let [header (first from)
                                        table-name (name (first header))
                                        header (mapv name (rest header))
                                        table (into [] (rest from))]
                                    `(.as (apply squery-jooq.operators/values ~table)
                                          ~table-name
                                          (into-array String ~header)))
                                  from)
                           from-coll (if (not (vector? from)) [from] from)]
                       (concat [`(squery-jooq.stages/from ~from-coll)] (rest qforms)))))
                 qforms)
        ;;for table result to be stored in a table alias (def nested-table (q :author :nested)) (q nested-table ..)
        [qforms table-alias ] (if (or (keyword? (last qforms)) (string? (last qforms)))
                               [(drop-last qforms) (name (last qforms))]
                               [qforms nil])
        ;;last always the select
        qforms (if (vector? (last qforms))
                 (let [last-form (last qforms)]
                   (concat [(if nested?
                              `(squery-jooq.stages/select-nested ~last-form)
                              `(squery-jooq.stages/select ~last-form))]
                           (butlast qforms)))
                 (concat [(if nested?
                            `(squery-jooq.stages/select-nested [])
                            `(squery-jooq.stages/select []))]
                         qforms))
        qforms (if table-alias
                 (concat qforms [`(.asTable ~table-alias)])
                 qforms)]
    qforms))

(defn pipeline
  "Converts a csql-pipeline to a mongo pipeline (1 vector with members stage operators)
   [],{}../nil  => empty stages or nil stages are removed
   [[] []] => [] []   flatten of stages (used when one stage produces more than 1 stages)
   cmql-filters combined =>  $match stage with $and
   [] projects  => $project"
  [csql-pipeline]
  (loop [csql-pipeline csql-pipeline
         after-group false
         sql-pipeline []]
    (if (empty? csql-pipeline)
      sql-pipeline
      (let [stage (first csql-pipeline)]
        (cond

          (or (= stage []) (nil? stage))                    ; ignore [] or nil stages
          (recur (rest csql-pipeline) after-group sql-pipeline)

          (and (list? stage) (list? (first stage)))
          (let [stage (into [] stage)
                stage (if after-group
                        `(squery-jooq.stages/having ~stage)
                        `(squery-jooq.stages/where ~stage))]
            (recur (rest csql-pipeline) after-group (conj sql-pipeline stage)))

          :else                                             ; any stage
          (if (and (list? stage) (= (first stage) 'group))
            (recur (rest csql-pipeline) true (conj sql-pipeline stage))
            (recur (rest csql-pipeline) after-group (conj sql-pipeline stage))))))))

(defn update-pipeline
  [csql-pipeline]
  (loop [csql-pipeline csql-pipeline
         sql-pipeline []]
    (if (empty? csql-pipeline)
      sql-pipeline
      (let [stage (first csql-pipeline)]
        (cond

          (or (= stage []) (nil? stage))                    ; ignore [] or nil stages
          (recur (rest csql-pipeline) sql-pipeline)

          (map? stage)                                      ; {:a ".." :!b ".."}
          (let [stage `(squery-jooq.stages/set-columns ~stage)]
            (recur (rest csql-pipeline) (conj sql-pipeline stage)))

          (and (list? stage) (list? (first stage)))
          (let [stage (into [] stage)
                stage `(squery-jooq.stages/where ~stage)]
            (recur (rest csql-pipeline) (conj sql-pipeline stage)))

          :else                                             ; any stage unchanged
          (recur (rest csql-pipeline) (conj sql-pipeline stage)))))))


(defn delete-pipeline
  [csql-pipeline]
  (loop [csql-pipeline csql-pipeline
         sql-pipeline []]
    (if (empty? csql-pipeline)
      sql-pipeline
      (let [stage (first csql-pipeline)]
        (cond

          (or (= stage []) (nil? stage))                    ; ignore [] or nil stages
          (recur (rest csql-pipeline) sql-pipeline)

          (and (list? stage) (list? (first stage)))
          (let [stage (into [] stage)
                stage `(squery-jooq.stages/where ~stage)]
            (recur (rest csql-pipeline) (conj sql-pipeline stage)))

          :else                                             ; any stage unchanged
          (recur (rest csql-pipeline) (conj sql-pipeline stage))
          )))))