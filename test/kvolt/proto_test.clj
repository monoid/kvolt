(ns kvolt.proto-test
  (:require [clojure.test :refer :all]
            [kvolt.proto :refer :all]))

;;;
;;; Setting...
;;;
(deftest parse-set-test
  (testing "Parsing simple set command"
    (is
     (= ["set" "key1" "42" "1000" "2000"]
        (parse-command-line "set key1 42 1000 2000\r\n")))))

(deftest parse-set-noreply-test
  (testing "Parsing noreply set command"
    (is
     (= ["set" "key1" "42" "1000" "2000" "noreply"]
        (parse-command-line "set key1 42 1000 2000 noreply\r\n")))))


(deftest parse-add-test
  (testing "Parsing simple add command"
    (is
     (= ["add" "key1" "42" "1000" "2000"]
        (parse-command-line "add key1 42 1000 2000\r\n")))))

(deftest parse-add-noreply-test
  (testing "Parsing noreply add command"
    (is
     (= ["add" "key1" "42" "1000" "2000" "noreply"]
        (parse-command-line "add key1 42 1000 2000 noreply\r\n")))))


(deftest parse-replace-test
  (testing "Parsing simple replace command"
    (is
     (= ["replace" "key1" "42" "1000" "2000"]
        (parse-command-line "replace key1 42 1000 2000\r\n")))))

(deftest parse-replace-noreply-test
  (testing "Parsing noreply replace command"
    (is
     (= ["replace" "key1" "42" "1000" "2000" "noreply"]
        (parse-command-line "replace key1 42 1000 2000 noreply\r\n")))))


(deftest parse-append-test
  (testing "Parsing simple append command"
    (is
     (= ["append" "key1" "42" "1000" "2000"]
        (parse-command-line "append key1 42 1000 2000\r\n")))))

(deftest parse-append-noreply-test
  (testing "Parsing noreply append command"
    (is
     (= ["append" "key1" "42" "1000" "2000" "noreply"]
        (parse-command-line "append key1 42 1000 2000 noreply\r\n")))))


(deftest parse-prepend-test
  (testing "Parsing simple prepend command"
    (is
     (= ["prepend" "key1" "42" "1000" "2000"]
        (parse-command-line "prepend key1 42 1000 2000\r\n")))))

(deftest parse-prepend-noreply-test
  (testing "Parsing noreply prepend command"
    (is
     (= ["prepend" "key1" "42" "1000" "2000" "noreply"]
        (parse-command-line "prepend key1 42 1000 2000 noreply\r\n")))))


(deftest parse-cas-test
  (testing "Parsing cas command."
    (is
     (= ["cas" "key1" "42" "1000" "2000" "3000"]
        (parse-command-line "cas key1 42 1000 2000 3000\r\n")))))


;;;
;;; get and gets
;;;
(deftest parse-get-test
  (testing "Parsing get command with single argument."
    (is
     (= ["get" "key1"]
        (parse-command-line "get key1\r\n")))))

(deftest parse-get4-test
  (testing "Parsing get command with 4 args."
    (is
     (= ["get" "key1" "key2" "key3" "key4"]
        (parse-command-line "get key1 key2 key3 key4\r\n")))))

(deftest parse-gets-test
  (testing "Parsing get command with single argument."
    (is
     (= ["gets" "key1"]
        (parse-command-line "gets key1\r\n")))))

(deftest parse-gets4-test
  (testing "Parsing get command with 4 args."
    (is
     (= ["gets" "key1" "key2" "key3" "key4"]
        (parse-command-line "gets key1 key2 key3 key4\r\n")))))

;;;
;;; delete
;;;
(deftest parse-delete-test
  (testing "Parsing simple delete command"
    (is
     (= ["delete" "key1"]
        (parse-command-line "delete key1\r\n")))))

(deftest parse-delete-noreply-test
  (testing "Parsing noreply delete command"
    (is
     (= ["delete" "key1" "noreply"]
        (parse-command-line "delete key1 noreply\r\n")))))

;;;
;;; incr/decr/touch
;;;
(deftest parse-incr-test
  (testing "Parsing simple incr command"
    (is
     (= ["incr" "key1" "100"]
        (parse-command-line "incr key1 100\r\n")))))

(deftest parse-incr-noreply-test
  (testing "Parsing noreply incr command"
    (is
     (= ["incr" "key1" "100" "noreply"]
        (parse-command-line "incr key1 100 noreply\r\n")))))

(deftest parse-decr-test
  (testing "Parsing simple decr command"
    (is
     (= ["decr" "key1" "100"]
        (parse-command-line "decr key1 100\r\n")))))

(deftest parse-decr-noreply-test
  (testing "Parsing noreply decr command"
    (is
     (= ["decr" "key1" "100" "noreply"]
        (parse-command-line "decr key1 100 noreply\r\n")))))

(deftest parse-touch-test
  (testing "Parsing simple touch command"
    (is
     (= ["touch" "key1" "100"]
        (parse-command-line "touch key1 100\r\n")))))

(deftest parse-touch-noreply-test
  (testing "Parsing noreply touch command"
    (is
     (= ["touch" "key1" "100" "noreply"]
        (parse-command-line "touch key1 100 noreply\r\n")))))

;;;
;;; Misc
;;;
(deftest parse-quit-test
  (testing "quit command"
    (is
     (= ["quit"]
        (parse-command-line "quit\r\n")))))

(deftest parse-version-test
  (testing "version command"
    (is
     (= ["version"]
        (parse-command-line "version\r\n")))))

(deftest parse-stats-test
  (testing "stats command"
    (is
     (= ["stats"]
        (parse-command-line "stats\r\n")))))

(deftest parse-stats-sizes-test
  (testing "stats with args command"
    (is
     (= ["stats" "sizes"]
        (parse-command-line "stats sizes\r\n")))))

(deftest parse-flush_all-simple-test
  (testing "simple flush_all command"
    (is
     (= ["flush_all"]
        (parse-command-line "flush_all\r\n")))))

(deftest parse-flush_all-arg-test
  (testing "flush_all with arg command"
    (is
     (= ["flush_all" "1000"]
        (parse-command-line "flush_all 1000\r\n")))))
