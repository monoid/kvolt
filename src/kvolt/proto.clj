(ns kvolt.proto
  (:require [lamina.core :refer :all]
            [aleph.tcp :refer :all]
            [gloss.core :refer :all]
            [kvolt.api :as api]))

(defn parse-command-line [cmd]
  (->> cmd
       (re-matches #"(?:(?:(set|add|replace|append|prepend) ([^ \t]+) (\d+) (\d+) (\d+)(?: (noreply))?)|(?:(get|gets) ([^ \t]+ )*([^ \t]+))) *\r\n")
       rest
       (filter identity)))

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
                             (repeat (Integer. (nth p 4)) :byte))
                            "\r\n"]]
                    (compile-frame fr)))
                (compile-frame [p]))
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
