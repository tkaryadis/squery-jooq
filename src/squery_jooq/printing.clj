(ns squery-jooq.printing
  (:require clojure.pprint
            [squery-jooq.utils.general :refer [isJavaArray?]])
  (:import (org.jooq Result)
           (java.util Arrays)))

(defn print-results [results]
  (if (instance? Result results)
    (println (.toString results))
    (println (.toString (.fetch results)))))

(defn print-sql [selectImpl]
  (println (.getSQL selectImpl))
  (prn))

#_(defn print-results-vec [results]
  (if (instance? Result results)
    (mapv (fn [r] (println
                    (mapv (fn [m]
                            (if (isJavaArray? m)
                              (Arrays/toString m)
                              m))
                          (.intoList r))))
          results)
    (mapv (fn [r] (println (mapv (fn [m]
                                   (if (isJavaArray? m)
                                     (Arrays/toString m)
                                     m))
                                 (.intoList r)))) (.fetch results)))
  (prn))

#_(defn print-results-map [results]
  (if (instance? Result results)
    (mapv (fn [r] (prn (.intoMap r))) results)
    (mapv (fn [r] (prn (.intoMap r))) (.fetch results)))
  (prn))
