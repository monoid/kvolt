(ns kvolt.api-test
  (:require [clojure.test :refer :all]
            [kvolt.api :refer :all])
  (:import clojure.lang.ExceptionInfo
           java.util.Arrays))

(def VALUE (.getBytes "value"))
(def ANOTHER (.getBytes "another"))

(deftest concat-byte-arrays-test
  (testing "Concatenation of byte arrays."
    (is (= [1 2 3 4 5]
           (vec (concat-byte-arrays (byte-array (map byte [1 2 3]))
                                    (byte-array (map byte [4 5]))))))))

(deftest empty-cache-test
  (testing "Querying from empty cache returns nothing."
    (is (= '()
           (cache-get (make-cache)
                      ["test"])))))

(deftest valid?-empty-test
  (testing "valid? returns false on non-existent entry."
    (let [c (make-cache), ts (System/currentTimeMillis)]
      (is (not (valid? ts c "test"))))))

(deftest valid?-zero-ts-test
  (testing "valid? returns true on entry with zero expiration."
    (let [c (make-cache), ts (System/currentTimeMillis)]
      (cache-set c "test" (byte-array []) 42 0)
      (is (valid? ts c "test")))))

(deftest valid?-old-test
  (testing "valid? returns false on expired entry."
    (let [c (make-cache), ts (System/currentTimeMillis)]
      (cache-set c "test" (byte-array []) 42 (- ts 100))
      (is (not (valid? ts c "test"))))))

(deftest valid?-new-test
  (testing "valid? returns true on fresh entry."
    (let [c (make-cache), ts (System/currentTimeMillis)]
      (cache-set c "test" (byte-array []) 42 (+ ts 10000))
      (is (valid? ts c "test")))))


(deftest set-and-get-key-test
  (testing "Setting and getting value."
    (let [c (make-cache)]
      (cache-set c "test" VALUE 0 0)
      (is (= "test"
             (-> (cache-get c ["test"])
                 first
                 (nth 0)))))))

(deftest set-and-get-value-test
  (testing "Setting and getting value."
    (let [c (make-cache)]
      (cache-set c "test" VALUE 0 0)
      (is (= VALUE
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest set-and-get-flags-test
  (testing "Setting and getting value."
    (let [c (make-cache)]
      (cache-set c "test" VALUE 42 0)
      (is (= '42
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :flags))))))

(deftest delete-test
  (testing "cache-delete."
    (let [c (make-cache)]
      (cache-set c "test" VALUE 42 0)
      (cache-delete c "test")
      (is (= '() (cache-get c ["test"]))))))

(deftest add-success-test
  (testing "cache-add success."
    (let [c (make-cache)]
      (cache-add c "test" VALUE 42 0)
      (is (Arrays/equals VALUE
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest add-failure-test
  (testing "cache-add failure on existring entry."
    (let [c (make-cache)]
      (cache-set c "test" VALUE 0 0)
      (is (thrown-with-msg? ExceptionInfo #"^NOT_STORED$"
                            (cache-add c "test" ANOTHER 42 0))))))

(deftest replace-success-test
  (testing "cache-add success."
    (let [c (make-cache)]
      (cache-set c "test" VALUE 0 0)
      (cache-replace c "test" ANOTHER 42 0)
      (is (Arrays/equals ANOTHER
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest replace-failure-test
  (testing "cache-add failure on existring entry."
    (let [c (make-cache)]
      (is (thrown-with-msg? ExceptionInfo #"^NOT_STORED$"
                            (cache-replace c "test" ANOTHER 42 0))))))

(deftest append-empty-test
  (testing "cache-append with empty cache."
    (let [c (make-cache)]
      (cache-append c "test" (.getBytes "value") 42 0)
      (is (Arrays/equals VALUE
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest append-nonempty-test
  (testing "cache-append with non-empty cache."
    (let [c (make-cache)]
      (cache-set c "test" (.getBytes "This is a") 42 0)
      (cache-append c "test" (.getBytes " text") 45 0)
      (is (Arrays/equals (.getBytes "This is a text")
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest prepend-empty-test
  (testing "cache-prepend with empty cache."
    (let [c (make-cache)]
      (cache-prepend c "test" VALUE 42 0)
      (is (Arrays/equals VALUE
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest prepend-nonempty-test
  (testing "cache-prepend with non-empty cache."
    (let [c (make-cache)]
      (cache-set c "test" (.getBytes "This is a") 42 0)
      (cache-prepend c "test" (.getBytes " text") 45 0)
      (is (Arrays/equals (.getBytes " textThis is a")
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest incr-success-test
  (testing "cache-incr for existing valid entry."
    (let [c (make-cache)]
      (cache-set c "test" (.getBytes "18") 42 0)
      (cache-incr c "test" (.getBytes "24"))
      (is (Arrays/equals (.getBytes "42")
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest incr-malformed1-test
  (testing "cache-incr for invalid value."
    (let [c (make-cache)]
      (cache-set c "test" (.getBytes "a18") 42 0)
      (is (thrown? NumberFormatException
                   (cache-incr c "test" (.getBytes "24")))))))

(deftest incr-malformed2-test
  (testing "cache-incr for invalid argument."
    (let [c (make-cache)]
      (cache-set c "test" (.getBytes "18") 42 0)
      (is (thrown? NumberFormatException
                   (cache-incr c "test" (.getBytes "a24")))))))

(deftest incr-nonexist-test
  (testing "cache-incr for empty cache."
    (let [c (make-cache)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"^NOT_FOUND$"
                            (cache-incr c "test" (.getBytes "42")))))))

(deftest decr-success-test
  (testing "cache-decr for existing valid entry without underflow."
    (let [c (make-cache)]
      (cache-set c "test" (.getBytes "24") 42 0)
      (cache-decr c "test" (.getBytes "18"))
      (is (Arrays/equals (.getBytes "6")
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest decr-success-test
  (testing "cache-decr for existing valid entry with underflow."
    (let [c (make-cache)]
      (cache-set c "test" (.getBytes "18") 42 0)
      (cache-decr c "test" (.getBytes "24"))
      (is (Arrays/equals (.getBytes "0")
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest decr-malformed1-test
  (testing "cache-decr for invalid value."
    (let [c (make-cache)]
      (cache-set c "test" (.getBytes "a18") 42 0)
      (is (thrown? IllegalArgumentException
                   (cache-decr c "test" (.getBytes "24")))))))

(deftest decr-malformed2-test
  (testing "cache-decr for invalid argument."
    (let [c (make-cache)]
      (cache-set c "test" (.getBytes "18") 42 0)
      (is (thrown? IllegalArgumentException
                   (cache-decr c "test" (.getBytes "a24")))))))

(deftest decr-nonexist-test
  (testing "cache-decr for empty cache."
    (let [c (make-cache)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"^NOT_FOUND$"
                            (cache-decr c "test" (.getBytes "42")))))))

(deftest flush-all-test
  (testing "flush-all."
    (let [c (make-cache)]
      (doseq [i (range 10)]
        (cache-set c (str "test" i) (.getBytes (str "value" i)) 42 0))
      (cache-flush-all c)
      (is (= '() (cache-get c "test5"))))))
