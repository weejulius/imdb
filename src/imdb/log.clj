(ns  ^{:doc "store the cmd log and can be used to rebuild index or store"}
  imdb.log
  (:require [imdb.protocol :as p]
            [imdb.boot :as b])
  (:use [clojure.test]))


(def logs "the logs of cmd" [])


(defn log-tx
  [tx-id pieces]
  (p/put! (b/get-state :log-db) tx-id pieces))


(deftest test-log-tx
  (testing ""
    (log-tx 121 {})
    (is (= {} (get (b/get-state :log-db) 121)))))
