(ns squery-jooq.reactor-utils.reactor
  (:use squery-mongoj-reactive.reactor-utils.functional-interfaces)
  (:import (clojure.lang MapEntry)
           (java.util Map$Entry)
           (java.util.concurrent CompletableFuture)
           (java.util.function BooleanSupplier)
           (java.util.stream Collectors)
           (reactor.core.publisher Flux Mono)
           (reactor.util.context Context)))

;;TODO
(defn mono-from-future
  ([fut]
   (let [cf (CompletableFuture.)
         _ (future (.complete cf @fut))]
     (Mono/fromFuture cf)))
  ([fut timeout-millis timeout-val]
   (let [cf (CompletableFuture.)
         _ (future (.complete cf (deref fut timeout-millis timeout-val)))]
     (Mono/fromFuture cf))))

(defn context-to-map [^Context context]
  (-> (.stream context)
      (.collect (Collectors/toMap
                  (ffn [^Map$Entry e] (.getKey e))
                  (ffn [^Map$Entry e] (.getValue e))))
      ((partial into {}))))







