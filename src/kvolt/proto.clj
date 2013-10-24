(ns kvolt.proto
  (:require [lamina.core :refer :all]
            [aleph.tcp :refer :all]
            [kvolt.api :as api]))

(defn- do-the-rap
  [ch client-info]
  (enqueue ch "HELLO, WORLD\r\n")
  (close ch))

(defn create-server
  [^Integer port]
  (let [cache (api/make-cache)]
    (start-tcp-server do-the-rap
                      {:port port})))
