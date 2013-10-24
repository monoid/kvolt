(ns kvolt.api-test
  (:require [clojure.test :refer :all]
            [kvolt.api :refer :all])
  (:import clojure.lang.ExceptionInfo))

(deftest empty-cache-test
  (testing "Querying from empty cache returns nothing."
    (is (= '()
           (cache-get (make-cache)
                      ["test"])))))

(deftest set-and-get-key-test
  (testing "Setting and getting value."
    (let [c (make-cache)]
      (cache-set c "test" "value" 0 0)
      (is (= "test"
             (-> (cache-get c ["test"])
                 first
                 (nth 0)))))))

(deftest set-and-get-value-test
  (testing "Setting and getting value."
    (let [c (make-cache)]
      (cache-set c "test" "value" 0 0)
      (is (= "value"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest set-and-get-flags-test
  (testing "Setting and getting value."
    (let [c (make-cache)]
      (cache-set c "test" "value" 42 0)
      (is (= '42
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :flags))))))

(deftest delete-test
  (testing "cache-delete."
    (let [c (make-cache)]
      (cache-set c "test" "value" 42 0)
      (cache-delete c "test")
      (is (= '() (cache-get c ["test"]))))))

(deftest add-success-test
  (testing "cache-add success."
    (let [c (make-cache)]
      (cache-add c "test" "value" 42 0)
      (is (= "value"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest add-failure-test
  (testing "cache-add failure on existring entry."
    (let [c (make-cache)]
      (cache-set c "test" "value" 0 0)
      (is (thrown-with-msg? ExceptionInfo #"^NOT_STORED$"
                            (cache-add c "test" "another" 42 0))))))

(deftest replace-success-test
  (testing "cache-add success."
    (let [c (make-cache)]
      (cache-set c "test" "value" 0 0)
      (cache-replace c "test" "another" 42 1)
      (is (= "another"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest replace-failure-test
  (testing "cache-add failure on existring entry."
    (let [c (make-cache)]
      (is (thrown-with-msg? ExceptionInfo #"^NOT_STORED$"
                            (cache-replace c "test" "another" 42 0))))))

(deftest append-empty-test
  (testing "cache-append with empty cache."
    (let [c (make-cache)]
      (cache-append c "test" "value" 42 0)
      (is (= "value"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest append-nonempty-test
  (testing "cache-append with non-empty cache."
    (let [c (make-cache)]
      (cache-set c "test" "This is a" 42 0)
      (cache-append c "test" " text" 45 1)
      (is (= "This is a text"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest prepend-empty-test
  (testing "cache-prepend with empty cache."
    (let [c (make-cache)]
      (cache-prepend c "test" "value" 42 0)
      (is (= "value"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest prepend-nonempty-test
  (testing "cache-prepend with non-empty cache."
    (let [c (make-cache)]
      (cache-set c "test" "This is a" 42 0)
      (cache-prepend c "test" " text" 45 1)
      (is (= " textThis is a"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest incr-success-test
  (testing "cache-incr for existing valid entry."
    (let [c (make-cache)]
      (cache-set c "test" "18" 42 0)
      (cache-incr c "test" "24")
      (is (= "42"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest incr-malformed1-test
  (testing "cache-incr for invalid value."
    (let [c (make-cache)]
      (cache-set c "test" "a18" 42 0)
      (is (thrown? NumberFormatException
                   (cache-incr c "test" "24"))))))

(deftest incr-malformed2-test
  (testing "cache-incr for invalid argument."
    (let [c (make-cache)]
      (cache-set c "test" "18" 42 0)
      (is (thrown? NumberFormatException
                   (cache-incr c "test" "a24"))))))

(deftest incr-nonexist-test
  (testing "cache-incr for empty cache."
    (let [c (make-cache)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"^NOT_FOUND$"
                            (cache-incr c "test" "42"))))))

(deftest decr-success-test
  (testing "cache-decr for existing valid entry without underflow."
    (let [c (make-cache)]
      (cache-set c "test" "24" 42 0)
      (cache-decr c "test" "18")
      (is (= "6"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest decr-success-test
  (testing "cache-decr for existing valid entry with underflow."
    (let [c (make-cache)]
      (cache-set c "test" "18" 42 0)
      (cache-decr c "test" "24")
      (is (= "0"
             (-> (cache-get c ["test"])
                 first
                 (nth 1)
                 :value))))))

(deftest decr-malformed1-test
  (testing "cache-decr for invalid value."
    (let [c (make-cache)]
      (cache-set c "test" "a18" 42 0)
      (is (thrown? IllegalArgumentException
                   (cache-decr c "test" "24"))))))

(deftest decr-malformed2-test
  (testing "cache-decr for invalid argument."
    (let [c (make-cache)]
      (cache-set c "test" "18" 42 0)
      (is (thrown? IllegalArgumentException
                   (cache-decr c "test" "a24"))))))

(deftest decr-nonexist-test
  (testing "cache-decr for empty cache."
    (let [c (make-cache)]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"^NOT_FOUND$"
                            (cache-decr c "test" "42"))))))
