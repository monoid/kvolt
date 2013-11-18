(ns kvolt.util
  )

(defn log [& args]
  (binding [*out* *err*]
    (apply println args)))
