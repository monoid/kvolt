(ns kvolt.proto
  (:require [clojure.string :as string]
            [lamina.core :refer :all]
            [aleph.tcp :refer :all]
            [gloss.core :refer :all]
            [gloss.io :refer :all]
            [kvolt.api :as api])
  (:import clojure.lang.ExceptionInfo))

(defn- split-varargs
  "Handle get and gets separately as regexps cannot parse their args in
ready form."
  [cmd]
  (case (nth cmd 0)
    ("get" "gets")
    (concat [(nth cmd 0)]
            (-> cmd
                (nth 1)
                (subs 1)
                (string/split #" ")))
    cmd))


(defn parse-command-line
  "Check validity of command line and split it into strings."
  [line]
  (some->> line
           (re-matches #"(?:(?:(set|add|replace|append|prepend) ([^ \t]+) (\d+) (\d+) (\d+)(?: (noreply))?)|(?:(cas) ([^ \t]+) (\d+) (\d+) (\d+) (\d+))|(?:(get|gets)((?: [^ \t]+)+))|(?:(stats)(?: ([^ \t]+))?)|(quit|version)|(?:(flush_all)(?: (\d+))?)|(?:(incr|decr|touch) ([^ \t]+) (\d+)(?: (noreply))?)|(?:(delete) ([^ \t]+)(?: (noreply))?))")
           rest ; remove first element
           (remove nil?)
           split-varargs))

(defcodec empty-frame
  [])

(def ^:const TEXT_CHARSET :ISO-8859-1)

(defcodec memcached-string
  (string TEXT_CHARSET :delimiters ["\r\n"]))

(defn memcached-bytes [n]
  ;; TODO finite-frame is broken in gloss 0.2.2 :(
  ;; (finite-frame n :byte)
  (compile-frame (repeat n :byte)))

(defcodec memcached-cmd
  (header memcached-string
          (fn [data]
            (if-let [p (seq (parse-command-line data))]
              (case (nth p 0)
                ("set" "add" "replace" "append" "prepend" "cas")
                (do
                  (let [fr [p
                            (memcached-bytes (Integer. (nth p 4)))
                            (string TEXT_CHARSET :length 0 :suffix "\r\n")
                            ]]
                    (compile-frame fr)))
                ;; Ok, it is some command without trailing data.
                (compile-frame [p]))
              ;; TODO: return error message, either ERROR (unknwn
              ;; command) or CLIENT_ERROR (known command with
              ;; incorrect args).
              empty-frame))
          (fn [p]
            (string/join " " (map str (first p))))))


(defn parse-value-line [line]
  (some->> line
           (re-matches #"VALUE ([^ \t]+) (\d+) (\d+)(?: (\d+))?")
           rest ; remove first element
           ))


;; Format of p:
;; [[key flags bytes] data]
(defcodec memcached-value-reply
  (header memcached-string
   (fn [data]
     (print data)
     (let [[k flags bytes maybe-cas] (parse-value-line data)]
       (print bytes)
       (compile-frame [[k flags bytes maybe-cas]
                       (memcached-bytes (Long. bytes))
                       (string TEXT_CHARSET :length 0 :suffix "\r\n")])))
   (fn [[[k flags bytes maybe-cas] body]]
     (if maybe-cas
       (format "VALUE %s %s %s %s" k flags bytes maybe-cas)
       (format "VALUE %s %s %s" k flags bytes)))))

(defcodec memcached-reply
  (header memcached-string
          (fn [data]
            (if (= 1 (count data))
              empty-frame
              ;; TODO multiplied by length! And with END\r\n
              memcached-value-reply)
            )
          (fn [p]
            (first p))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;
;;; Protocol command handlers
;;;

;;; We use api commands almost directly
(def ^:const STORAGE_MAP
  {"set" api/cache-set
   "add" api/cache-add
   "replace" api/cache-replace
   "append" api/cache-append
   "prepend" api/cache-prepend})

(defn proto-cas [cache & args]
  "NOT_FOUND\r\n")

;;; Other commands
(defn proto-get [store & keys]
  (for [[k e] (api/cache-get store keys)]
    [[k (:flags e) (alength (:value e)) nil]
     (seq (:value e))
     ""]))

(defn proto-gets [store keys]
  (for [[k e] (api/cache-get store keys)]
    [[k (:flags e) (alength (:value e)) (:store-ts e)]
     (seq (:value e))
     ""]))

(defn proto-delete [store key & noreply]
  [(try
     (api/cache-delete store key)
     (catch ExceptionInfo ex
       (.getMessage ex)))
   (boolean (seq noreply))])

(defn proto-incr [store key value & noreply]
  [(try
     (api/cache-incr store key value)
     (catch ExceptionInfo ex
       (.getMessage ex)))
   (boolean (seq noreply))])

(defn proto-decr [store key value & noreply]
  [(try
     (api/cache-decr store key value)
     (catch ExceptionInfo ex
       (.getMessage ex)))
   (boolean (seq noreply))])

(defn proto-touch [store key exptime & noreply]
  [(try
     (api/cache-touch store key exptime)
     (catch ExceptionInfo ex
       (.getMessage ex)))
   (boolean (seq noreply))])

(defn proto-stats [store & args]
  ["END\n\r" false])

(defn proto-flush-all [store & ts]
  [(if (seq ts)
     (api/cache-flush-all store (first ts))
     (api/cache-flush-all store))
   false])

(defn proto-verbosity [store val & noreply]
  ["ON\n\r" false])

(defn proto-version [store]
  ["VERSION kvolt-0.1.0" false])

(defn proto-quit [ch]
  (close ch))

(def ^:const OTHER_MAP
  {"delete" proto-delete
   "incr" proto-incr
   "decr" proto-decr
   "touch" proto-touch
   "stats" proto-stats
   "flush_all" proto-flush-all
   "verbosity" proto-verbosity
   "version" proto-version})

(defn handle-request
  [ch cache [[cmd & args] & maybe-data]]
  (case cmd
    ;; Commands with data
    ;; WARNING: data comes just after cache, not after key
    ("set" "add" "replace" "append" "prepend")
    (let [[key flags expire len & noreply] args]
      (some->>
       (try
         (do
           ((STORAGE_MAP cmd)
            cache
            key
            (byte-array (nth maybe-data 0))
            (Long. flags)
            (Long. expire))
           (if (seq noreply)
             ""
             "STORED"))
         (catch ExceptionInfo ex
           (if (seq noreply)
             ""
             (.getMessage ex))))
       (encode (if noreply
                 empty-frame
                 memcached-string))
       (enqueue ch)))

    ;; cas has different arguments
    "cas"
    (let [noreply (< 6 (count args))]
      (try
        (apply proto-cas cache  (byte-array (nth maybe-data 0)) args)

        (when-not noreply
          "STORED")
        (catch ExceptionInfo ex
          (when-not noreply
            (.getMessage ex)))))

    "quit"
    (proto-quit ch)

    ("get" "gets")
    (let [values (apply
                  (if (= cmd "get")
                    proto-get
                    proto-gets)
                  cache args)]
      (->> values
           (encode-all memcached-value-reply)
           (enqueue ch))
      (->> "END"
           (encode memcached-string)
           (enqueue ch)))

    ;; Otherwise
    (try
      (let [[data noreply]
            (apply (OTHER_MAP cmd) cache args)]
        (if noreply
          empty-frame
          data))
      (catch ExceptionInfo ex
        (.getMessage ex)))))


(defn- do-the-rap
  [cache ch client-info]
  (receive-all ch
               (partial handle-request ch cache)))

(defn create-server
  [^Integer port]
  (let [cache (api/make-cache)]
    (start-tcp-server (partial do-the-rap cache)
                      {:port port
                       :decoder memcached-cmd})))
