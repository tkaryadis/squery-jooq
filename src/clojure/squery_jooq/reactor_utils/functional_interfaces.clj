(ns squery-jooq.reactor-utils.functional-interfaces)

(defmacro cfn
  ([vec-args body]
   `(reify
      java.util.function.Consumer
      (accept [this# arg#]
        ((fn ~vec-args ~body) arg#))))
  ([f]
   `(reify
      java.util.function.Consumer
      (accept [this# arg#]
        (~f arg#)))))

(defmacro lcfn
  ([vec-args body]
   `(reify
      java.util.function.LongConsumer
      (accept [this# arg#]
        ((fn ~vec-args ~body) arg#))))
  ([f]
   `(reify
      java.util.function.LongConsumer
      (accept [this# arg#]
        (~f arg#)))))

(defmacro ffn
  ([vec-args body]
   `(reify
      java.util.function.Function
      (apply [this# arg#]
        ((fn ~vec-args ~body) arg#))))
  ([f]
   `(reify
      java.util.function.Function
      (apply [this# arg#]
        (~f arg#)))))

(defmacro pfn
  ([vec-args body]
   `(reify
      java.util.function.Predicate
      (test [this# arg#]
        ((fn ~vec-args ~body) arg#))))
  ([f]
   `(reify
      java.util.function.Predicate
      (test [this# arg#]
        (~f arg#)))))

(defmacro bfn
  ([vec-args body]
   `(reify
      java.util.function.BiFunction
      (apply [this# arg1# arg2#]
        ((fn ~vec-args ~body) arg1# arg2#))))
  ([f]
   `(reify
      java.util.function.BiFunction
      (apply [this# arg1# arg2#]
        (~f arg1# arg2#)))))

(defmacro bcfn
  ([vec-args body]
   `(reify
      java.util.function.BiConsumer
      (accept [this# arg1# arg2#]
        ((fn ~vec-args ~body) arg1# arg2#))))
  ([f]
   `(reify
      java.util.function.BiConsumer
      (accept [this# arg1# arg2#]
        (~f arg1# arg2#)))))

(defmacro sfn
  ([vec-args body]
   `(reify
      java.util.function.Supplier
      (get [this#]
        ((fn ~vec-args ~body)))))
  ([f]
   `(reify
      java.util.function.Supplier
      (get [this#]
        (~f)))))

;;positive integer if o1 > o2
;;negative integer if o1 < o2
;;0 if o1=o2
(defmacro compfn
  ([vec-args body]
   `(reify
      java.util.Comparator
      (compare [this# arg1# arg2#]
        ((fn ~vec-args ~body) arg1# arg2#))))
  ([f]
   `(reify
      java.util.Comparator
      (compare [this# arg1# arg2#]
        (~f arg1# arg2#)))))

(defmacro boolfn
  ([vec-args body]
   `(reify
      java.util.function.BooleanSupplier
      (getAsBoolean [this#]
        ((fn ~vec-args ~body)))))
  ([f]
   `(reify
      java.util.function.BooleanSupplier
      (getAsBoolean [this#]
        (~f)))))

(defmacro uopfn
  ([vec-args body]
   `(reify
      java.util.function.UnaryOperator
      (apply [this# arg#]
        ((fn ~vec-args ~body) arg#))))
  ([f]
   `(reify
      java.util.function.UnaryOperator
      (apply [this# arg#]
        (~f arg#)))))

(defmacro bopfn
  ([vec-args body]
   `(reify
      java.util.function.BinaryOperator
      (apply [this# arg1# arg2#]
        ((fn ~vec-args ~body) arg1# arg2#))))
  ([f]
   `(reify
      java.util.function.BinaryOperator
      (apply [this# arg1# arg2#]
        (~f arg1# arg2#)))))

(defmacro intfn
  ([vec-args body]
   `(reify
      java.util.function.ToIntFunction
      (applyAsInt [this# arg#]
        ((fn ~vec-args ~body) arg#))))
  ([f]
   `(reify
      java.util.function.ToIntFunction
      (applyAsInt [this# arg#]
        (~f arg#)))))