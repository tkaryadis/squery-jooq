(ns squery-jooq.printing
  (:require clojure.pprint
            [squery-jooq.utils :refer [isJavaArray?]])
  (:import (org.jooq Result)
           (java.util Arrays)))

(defn print-results [results]
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

(defn print-json-results [results]
  (if (instance? Result results)
    (mapv (fn [r] (prn (.intoMap r))) results)
    (mapv (fn [r] (prn (.intoMap r))) (.fetch results)))
  (prn))

(defn print-sql [selectImpl]
  (println (.getSQL selectImpl))
  (prn))