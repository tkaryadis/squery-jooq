(ns squery-jooq.reactor-utils.jooq

  (:require [squery-jooq.utils.utils :refer [record-to-map]])
  (:import (reactor.core.publisher Flux)))

(defn flux-records [q-result]
  (-> (Flux/from q-result)
      (.map (fn [r] (record-to-map r)))))