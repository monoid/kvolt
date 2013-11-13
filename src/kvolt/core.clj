(ns kvolt.core
  (:use kvolt.proto)
  (:gen-class))

(def ^:const PORT 11211)

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  ;; work around dangerous default behaviour in Clojure
  ;; Commented out because aleph fails to work with this line.
  ;(alter-var-root #'*read-eval* (constantly false))
  (println (str "Running server on port " PORT "."))
  (create-server PORT))
