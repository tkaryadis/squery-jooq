(ns squery-jooq.utils.utils)


(defn record-to-map [record]
     (reduce (fn [v t]
               (assoc v (keyword (.getName t)) (.get record t)))
             {}
             (.fields record)))