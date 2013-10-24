(ns kvolt.api
  (:require [clojure.java.io :as io]))

(defn make-cache []
  (atom {}))

(defrecord Entry
    ;; TODO: is using store-ts for CAS a good idea?
    [value flags store-ts access-ts expire])

(defn- make-entry [value flags expire]
  (let [time (System/currentTimeMillis)]
    (->Entry value flags time time expire)))

(defn- update-access-ts
  [entry ts-func]
  (update-in entry [:access-ts] ts-func))

(defn- update-entries [c keys f & values]
  (reduce (fn [c k]
            (if (contains? c k)
              (apply update-in c [k] f values)
              c))
          c
          keys))

(defn cache-get
  "GET and GETS commands.  Returns seq of arrays [key, Entry]."
  [cache keys]
  ;; Update timestamps and return new cache.
  (let [c (swap! cache
                 (fn [c keys]
                   (let [ts-func (constantly (System/currentTimeMillis))]
                     (update-entries c keys update-access-ts ts-func)))
                 keys)]
    (filter second ; Remove entries without values
            (map (juxt identity c) keys))))

(defn cache-delete
  "DELETE command."
  [cache key]
  (swap! cache
         (fn [c]
           (if (contains? c key)
             (dissoc c key)
             (throw (ex-info "NOT_FOUND" {:key key :cache c}))))))

(defn cache-set
  "SET command."
  [cache key value flags expire]
  (swap! cache
         assoc
         key
         (make-entry value flags expire)))

(defn cache-add
  "ADD command."
  [cache key value flags expire]
  (swap! cache
         (fn [c]
           (if (contains? c key)
             (throw (ex-info "NOT_STORED" {:key key :value value
                                           :flags flags :expire expire
                                           :cache c}))
             (assoc c key (make-entry value flags expire))))))


(defn cache-replace
  "REPLACE command."
  [cache key value flags expire]
  (swap! cache
         (fn [c]
           (if (contains? c key)
             (assoc c key (make-entry value flags expire))
             (throw (ex-info "NOT_STORED" {:key key :value value
                                           :flags flags :expire expire
                                           :cache c}))))))

(defn- update-with-func
  "Update entry with function that accepts new and old values.
Long form creates entry if it doesn't exist, short throws \"NOT_FOUND\"."
  ([cache key value func]
     (swap! cache
            (fn [c]
              (assoc c key
                     ;; INCR and DECR
                     (if (contains? c key)
                       (let [e (get c key)]
                         (make-entry (func (:value e) value)
                                     (:flags e)
                                     (:expire e)))
                       (throw (ex-info "NOT_FOUND" {:key key
                                                    :value value})))))))
  ([cache key value flags expire func default]
     (swap! cache
            (fn [c]
              (assoc c key
                     ;; APPEND and PREPEND
                     (make-entry (func (:value (get c key
                                                    {:value default}))
                                       value)
                                 flags expire))))))

(defn cache-append
  "APPEND command."
  [cache key value flags expire]
  (update-with-func cache key value flags expire #(str %1 %2) ""))

(defn cache-prepend
  "PREPEND command."
  [cache key value flags expire]
  (update-with-func cache key value flags expire #(str %2 %1) ""))

(defn cache-incr
  "INCR command."
  [cache key value]
  (update-with-func cache key value
                    (fn [old new]
                      (str (+ (Long. old) (Long. new))))))

(defn cache-decr
  "DECR command."
  [cache key value]
  (update-with-func cache key value
                    (fn [old new]
                      (str
                       ;; Prevent underflow as per spec
                       (max 0
                            (- (Long. old) (Long. new)))))))
