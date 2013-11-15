(ns kvolt.api
  (:require [clojure.java.io :as io]))

(def ^:const MONTH (* 30 24 60 60 1000))

(defn make-cache []
  {::data (atom {})
   ::cas (atom 0)}) ; CAS counter

(defn resolve-time
  ([ts]
     (resolve-time (System/currentTimeMillis) ts))
  ([time ts]
     (cond
      (zero? time) 0                    ; Zero is a special value.
      (< time MONTH) (+ time ts)        ; Relative timestamp.
      :default time)))                  ; Absolute timestamp.

(defrecord Entry
    ;; TODO: is using store-ts for CAS a good idea?
    [value flags cas expire])

(defn- make-entry [value flags expire cas]
  (let [ts (System/currentTimeMillis)]
    (->Entry value flags cas (resolve-time expire ts))))

(defn- update-access-ts
  [entry ts]
  (assoc-in entry [:access-ts] ts))

(defn valid?
  "Check if entry is valid, i.e. exists and is not expired."
  ([ts e]
     (and e (let [ex (:expire e)]
              (or (zero? ex)
                  (>= ex ts)))))
  ([ts cache key]
     (let [c (get cache ::data cache)]
       (valid? ts (get (if (instance? clojure.lang.Atom c)
                         (deref c)
                         c)
                       key)))))

(defn- update-entries [c keys f & values]
  (let [ts (System/currentTimeMillis)]
    (reduce (fn [c k]
              (if (valid? ts c k)
                (apply update-in c [k] f values)
                c))
            c
            keys)))

(defn filter-entries
  "Filter entries.  Function f takes key and entry, returning boolean."
  [func m]
  (hash (filter (comp apply func) m)))

(defn filter-entries--select
  [func m]
  (->> (keys m)
       (filter #(func % (m %)))
       (select-keys m))
  (select-keys m
               (filter #(func % (m %))
                       (keys m))))

(defn filter-entries--dissoc
  [func m]
  (reduce (fn [c k]
            (if (func k (c k))
              c
              (dissoc c k)))
          m
          (keys m)))


(defn cache-get
  "GET and GETS commands.  Returns seq of arrays [key, Entry]."
  [cache keys]
  ;; Update timestamps and return new cache.
  (let [ts (System/currentTimeMillis)
        c @(::data cache)]
    (filter (comp (partial valid? ts)   ; Remove invalid values
                  second)
            (map (juxt identity c) keys))))

(defn cache-delete
  "DELETE command."
  [cache key]
  (let [ts (System/currentTimeMillis)]
    (swap! (::data cache)
           (fn [c]
             (if (valid? ts c key)
               (dissoc c key)
               (throw (ex-info "NOT_FOUND" {:key key :cache c})))))))

(defn cache-set
  "SET command."
  [cache key value flags expire]
  (let [cas (swap! (::cas cache) inc)]
    (swap! (::data cache)
           assoc
           key
           (make-entry value flags expire cas))))

(defn cache-add
  "ADD command."
  [cache key value flags expire]
  (let [ts (System/currentTimeMillis)
        cas (swap! (::cas cache) inc)]
    (swap! (::data cache)
           (fn [c]
             (if (valid? ts c key)
               (throw (ex-info "NOT_STORED" {:key key :value value
                                             :flags flags :expire expire
                                             :cache c}))
               (assoc c key (make-entry value flags expire cas)))))))


(defn cache-replace
  "REPLACE command."
  [cache key value flags expire]
  (let [ts (System/currentTimeMillis)
        cas (swap! (::cas cache) inc)]
    (swap! (::data cache)
           (fn [c]
             (if (valid? ts c key)
               (assoc c key (make-entry value flags expire cas))
               (throw (ex-info "NOT_STORED" {:key key :value value
                                             :flags flags :expire expire
                                             :cache c})))))))

(defn- update-with-func
  "Update entry with function that accepts new and old values.
Long form creates entry if it doesn't exist, short throws \"NOT_FOUND\"."
  ([cache key value func]
     (let [ts (System/currentTimeMillis)
           cas (swap! (::cas cache) inc)]
       (swap! (::data cache)
              (fn [c]
                (assoc c key
                       ;; INCR and DECR
                       (let [e (get c key)]
                         (if (valid? ts e)
                           (make-entry (func (:value e) value)
                                       (:flags e)
                                       (:expire e)
                                       cas)
                           (throw (ex-info "NOT_FOUND" {:key key
                                                        :value value})))))))))
  ([cache key value flags expire func default]
     (let [ts (System/currentTimeMillis)
           cas (swap! (::cas cache) inc)]
       (swap! (::data cache)
              (fn [c]
                (assoc c key
                       ;; APPEND and PREPEND
                       (make-entry (func
                                    (let [e (get c key)]
                                      (if (valid? ts e)
                                        (:value e)
                                        default))
                                    value)
                                   flags expire cas)))))))

(defn concat-byte-arrays
  ([^bytes a ^bytes b]
     (let [al (alength a)
           bl (alength b)
           r (byte-array (+ al bl))]
       (System/arraycopy a 0 r  0 al)
       (System/arraycopy b 0 r al bl)
       r))
  ([^bytes a ^bytes b ^bytes c]
     (let [al (alength a)
           bl (alength b)
           cl (alength c)
           r (byte-array (+ al bl cl))]
       (System/arraycopy a 0 r  0 al)
       (System/arraycopy b 0 r al bl)
       (System/arraycopy c 0 r (+ al bl) cl)
       r)))

(defn cache-append
  "APPEND command."
  [cache key value flags expire]
  (update-with-func cache key value flags expire #(concat-byte-arrays %1 %2)
                    (byte-array [])))

(defn cache-prepend
  "PREPEND command."
  [cache key value flags expire]
  (update-with-func cache key value flags expire #(concat-byte-arrays %2 %1)
                    (byte-array [])))

(defn cache-cas
  "CAS command."
  [cache key value flags expire cas]
  (swap! (::data cache)
         #(if-let [e (% key)]
            (if (= cas (:cas e))
              (assoc % key (make-entry value flags expire (swap!
                                                           (::cas cache)
                                                           inc)))
              (throw (ex-info "EXISTS" {:key key :cas cas :new-cas (:cas e)})))
            (throw (ex-info "NOT_FOUND" {:key key :cas cas})))))

(defn cache-incr
  "INCR command."
  [cache key value]
  (get-in
   (update-with-func cache key value
                     (fn [old new]
                       (.getBytes
                        (str (+ (Long. (String. old))
                                (Long. (String. new)))))))
   [key :value]))

(defn cache-decr
  "DECR command."
  [cache key value]
  (get-in
   (update-with-func cache key value
                     (fn [old new]
                       (.getBytes
                        (str
                         ;; Prevent underflow as per spec
                         (max 0
                              (- (Long. (String. old))
                                 (Long. (String. new))))))))
   [key :value]))

(defn cache-touch [cache key expire]
  (let [ts (System/currentTimeMillis)
        expire (resolve-time ts (Long. expire))]
    (swap! (::data cache)
           (fn [c]
             (let [e (c key)]
               (if (valid? ts e)
                 ;; TODO: same CAS
                 (assoc c key (-> e
                                  (assoc :expire expire)
                                  (assoc :access-ts ts)))
                 (throw (ex-info "NOT_FOUND" {:key key
                                              :expire expire
                                              :cache c}))))))))

(defn cache-flush-all
  ([cache]
     (reset! (::data cache) {}))
  ([cache ts]
     (let [ts (resolve-time (Long. ts))]
       (swap! (::data cache) filter-entries #(valid? ts %2)))))

(defn cache-gc
  "Remove expired entries."
  [cache]
  (cache-flush-all cache (System/currentTimeMillis)))
