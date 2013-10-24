(ns kvolt.core-test
  (:require [clojure.test :refer :all]
            [kvolt.core :refer :all]))

(deftest a-test
  (testing "Fixed, I don't fail."
    (is (= 0 0))))
