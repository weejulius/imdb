(ns  ^{:doc "store the cmd log and can be used to rebuild index or store"}
  imdb.log
  (:require [clj-leveldb :as leveldb]
            [imdb.boot :as b])
  (:use [clojure.test]))


(def logs "the logs of cmd" [])


(defn log-tx
  [tx-id pieces]
  (leveldb/put (b/get-state :log-db) tx-id pieces))


(deftest test-log-tx
  (testing ""
    (log-tx 121 {})
    (is (= {} (leveldb/get (b/get-state :log-db) 121)))))
