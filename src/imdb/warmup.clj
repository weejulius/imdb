(ns imdb.warmup
  (:require [imdb.boot :as b]
            [clj-leveldb :as leveldb]
            [common.convert :as cvt]))

(defn start-tx-db
  [state]
  (b/attach :log-db (leveldb/create-db
                     (b/get-state :tx-db-path "/tmp/tx-db5")
                     {:key-encoder cvt/->bytes
                      :val-encoder cvt/->bytes
                      :key-decoder cvt/->long
                      :val-decoder cvt/->data})))

(defn stop-tx-db
  [state]
  (when (b/get-state :log-db)
    (.close (b/get-state :log-db))
    (b/dis-attach :log-db)))


(defn refresh []
  (b/refresh!
   (fn []
     (b/add-lifecycle start-tx-db stop-tx-db))))
