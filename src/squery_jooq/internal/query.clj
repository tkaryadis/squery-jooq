(ns squery-jooq.internal.query
  (:require                                                 ;squery-jooq.stages
            [squery-jooq.internal.common :refer [columns]])
  (:import (org.jooq.impl DSL)))

(defn switch-select-from [qforms from-also?]
  (let [;;first always the from
        qforms (if from-also?
                 (let [from (first qforms)
                       from-coll (if (not (vector? from)) [from] from)]
                   (concat [`(squery-jooq.stages/from ~from-coll)] (rest qforms)))
                 qforms)
        ;;last always the select
        qforms (if (vector? (last qforms))
                 (let [last-form (last qforms)]
                   (concat [`(squery-jooq.stages/select ~last-form)] (butlast qforms)))
                 (concat [`(squery-jooq.stages/select [])] qforms))]
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