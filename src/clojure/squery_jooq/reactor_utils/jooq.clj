(ns squery-jooq.reactor-utils.jooq
  (:use squery-jooq.reactor-utils.functional-interfaces)
  (:require [squery-jooq.utils.utils :refer [record-to-map]])
  (:import (reactor.core.publisher Flux)))

(defn flux-records [q-result]
  (-> (Flux/from q-result)
      (.map (ffn [r] (record-to-map r)))))