(ns ^{:doc "garantee the acid of the execution of cmd, it can be turn off for some entity"}
  imdb.transaction
  (:require [imdb.id-creator :as idc]
            [imdb.log :as log])
  (:use [clojure.test]))

;; # Flow
;; 1. (async) log the transaction in order to replay the logs to rebuild index
;; 2. mark the transaction
;; 3. build index and persist
;; 4. unmark the transaction
;; 5. update the log for the transaction
;; 6. in case the transaction is failed, rollback the changes


;; # Design
;; create id for each transaction
;; mark the transaction started firstly
;; tied the transaction id to every data
;; when things are done
;; mark the transaction done
;; otherwise the data does not take affect
;; rebuild index will throw those dirty data away

(defn gen-tx-id
  "generate the id for transaction"
  []
  (idc/gen-id :tx))


(def tx (atom {}))

(defn log-tx
  "log the transcaction at the beginning, but now it is not nessesary"
  [tx-id pieces])

(defn log-tx-done
  "log the done of the transaction"
  [tx-id pieces]
  (log/log-tx tx-id pieces))

(defn mark-tx
  [tx-id pieces]
  (log-tx tx-id pieces)
  (swap! tx (fn [c]
              (assoc c tx-id 0))))

(defn unmark-tx
  "unmark the transaction when it is done
   TODO: it should be atomic, but now it is not"
  [tx-id pieces]
  (log-tx-done tx-id pieces)
  (swap! tx (fn [c]
              (dissoc c tx-id))))

(defn tx-done?
  "whether the tx the piece tied to is done?"
  [tx-id]
  (nil? (get @tx tx-id)))

(defn tie-to-tx
  [tx-id pieces]
  (if (seq? pieces)
    (map #(assoc % :tx-id tx-id) pieces)
    (assoc pieces :tx-id tx-id)))

(defn run-tx
  "run a transaction"
  [pieces f]
  (let [tx-id (gen-tx-id)
        pieces (tie-to-tx tx-id pieces)]
    (mark-tx tx-id pieces)
    (f pieces)
    (unmark-tx tx-id pieces)))

(def piece-example
  {:eid 1212122
   :entity :user
   :id 121212
   :key :name
   :val "bob"})

(deftest test-tie-to-tx
  (testing ""
    (is (= 1212 (:tx-id (tie-to-tx 1212 piece-example))))))
