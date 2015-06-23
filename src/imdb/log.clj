(ns  ^{:doc "store the cmd log and can be used to rebuild index or store"}
  imdb.log
  (:require [clj-leveldb :as leveldb]
            [common.convert :as cvt])
  (:use [clojure.test]))


(def logs "the logs of cmd" [])

(def store (leveldb/create-db "/tmp/tx-log" {:key-encoder cvt/->bytes
                                              :val-encoder cvt/->bytes
                                              :key-decoder cvt/->long
                                              :val-decoder cvt/->data}))

(defn log-tx
  [tx-id pieces]
  (leveldb/put store tx-id pieces))

(defn stop
  []
  (.close store))

(deftest test-log-tx
  (testing ""
    (log-tx 121 {})
    (is (= {} (leveldb/get store 121)))))
