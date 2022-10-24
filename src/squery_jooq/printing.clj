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