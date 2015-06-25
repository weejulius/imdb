(ns  ^{:doc "store the cmd log and can be used to rebuild index or store"}
  imdb.log
  (:require [clj-leveldb :as leveldb]
            [common.convert :as cvt]
            [imdb.boot :as b])
  (:use [clojure.test]))


(def logs "the logs of cmd" [])

(b/before!
 (fn [state]
   (b/attach :log-db (leveldb/create-db (b/get-state :tx-db-path "/tmp/tx-db-test")
                                        {:key-encoder cvt/->bytes
                                         :val-encoder cvt/->bytes
                                         :key-decoder cvt/->long
                                         :val-decoder cvt/->data}))
   state))

(b/after!
 (fn [state]
   (when (b/get-state :log-db)
     (.close (b/get-state :log-db))
     (b/dis-attach :log-db))
   state))


(defn log-tx
  [tx-id pieces]
  (leveldb/put (b/get-state :log-db) tx-id pieces))


(deftest test-log-tx
  (testing ""
    (log-tx 121 {})
    (is (= {} (leveldb/get (b/get-state :log-db) 121)))))
