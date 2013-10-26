(ns kvolt.proto
  (:require [clojure.string :as string]
            [lamina.core :refer :all]
            [aleph.tcp :refer :all]
            [gloss.core :refer :all]
            [kvolt.api :as api]))

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
           (re-matches #"(?:(?:(set|add|replace|append|prepend) ([^ \t]+) (\d+) (\d+) (\d+)(?: (noreply))?)|(?:(cas) ([^ \t]+) (\d+) (\d+) (\d+) (\d+))|(?:(get|gets)((?: [^ \t]+)+))|(?:(stats)(?: ([^ \t]+))?)|(quit|version)|(?:(flush_all)(?: (\d+))?)|(?:(incr|decr|touch) ([^ \t]+) (\d+)(?: (noreply))?)|(?:(delete) ([^ \t]+)(?: (noreply))?)) *\r\n")
           rest ; remove first element
           (remove nil?)
           split-varargs))

(defcodec empty-frame
  [])

(defcodec memcached-cmd
  (header (string :utf-8 :delimeters [" " "\r\n"])
          (fn [data]
            (println data)
            (if-let [p (seq (parse-command-line data))]
              (case (nth p 0)
                ("set" "add" "replace" "append" "prepend" "cas")
                (do
                  (let [fr [p
                            (compile-frame
                             ;; Is there better way to represent array
                             ;; of n bytes in glos?
                             ;; (fixed-frame n) failed with cryptic message.
                             (repeat (Integer. (nth p 4)) :byte))
                            "\r\n"]]
                    (compile-frame fr)))
                ;; Ok, it is some command without trailing data.
                (compile-frame [p]))
              ;; TODO: return error message, either ERROR (unknwn
              ;; command) or CLIENT_ERROR (known command with
              ;; incorrect args).
              empty-frame))
          (fn [& args]
            ;; TODO TODO TODO
            (println "encoder" args)
            empty-frame)))

(defn- do-the-rap
  [ch client-info]
  (receive-all ch
               println))

(defn create-server
  [^Integer port]
  (let [cache (api/make-cache)]
    (start-tcp-server do-the-rap
                      {:port port
                       :frame memcached-cmd})))
