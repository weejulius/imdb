(ns imdb.warmup
  (:require [imdb.boot :as b]
            [clj-leveldb :as leveldb]
            [common.convert :as cvt]))

(defn start-tx-db
  [state]
  (b/attach :log-db (leveldb/create-db
                     (b/get-state :tx-db-path "/tmp/tx-db4")
                     {:key-encoder cvt/->bytes
                      :val-encoder cvt/->bytes
                      :key-decoder cvt/->long
                      :val-decoder cvt/->data})))

(defn start-schema-db
  [state]
  (b/attach :schema-db (leveldb/create-db
                        (b/get-state :schema-db-path "/tmp/schema-db4")
                        {:key-encoder cvt/->bytes
                         :val-encoder cvt/->bytes
                         :key-decoder cvt/->long
                         :val-decoder cvt/->data})))


(defn stop-db
  [key state]
  (when (b/get-state key)
    (.close (b/get-state key))
    (b/dis-attach key)))


(defn refresh []
  (b/refresh!
   (fn []
     (b/add-lifecycle start-tx-db (partial stop-db :log-db))
     (b/add-lifecycle start-schema-db (partial stop-db :schema-db)))))
