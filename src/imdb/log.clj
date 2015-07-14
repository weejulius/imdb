(ns  ^{:doc "store the cmd log and can be used to rebuild index or store"}
  imdb.log
  (:require [imdb.protocol :as p]
            [imdb.boot :as b])
  (:use [clojure.test]))


(def logs "the logs of cmd" [])


(defn log-tx
  [db tx-id pieces]
  (p/put! db tx-id pieces))
