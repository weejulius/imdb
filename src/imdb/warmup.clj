(ns imdb.warmup
  (:require [imdb.boot :as b]
            [imdb.store :as store]
            [imdb.protocol :as p]
            [common.convert :as cvt]))

(defn start-tx-db
  [state]
  (store/create-store :log-db
                      (b/get-state :tx-db-path "/tmp/tx-db1")
                      {:key-encoder cvt/->bytes
                       :val-encoder cvt/->bytes
                       :key-decoder cvt/->long
                       :val-decoder cvt/->data} ))

(defn start-schema-db
  [state]
  (store/create-store :schema-db
                      (b/get-state :tx-db-path "/tmp/schema-db1")
                      {:key-encoder cvt/->bytes
                       :val-encoder cvt/->bytes
                       :key-decoder cvt/->long
                       :val-decoder cvt/->data}))

(defn start-pieces-db
  [state]
  (store/create-store :pieces-db
                      (b/get-state :tx-db-path "/tmp/pieces-db1")
                      {:key-encoder cvt/->bytes
                       :val-encoder cvt/->bytes
                       :key-decoder cvt/->long
                       :val-decoder cvt/->data}))



(defn stop-db
  [key state]
  (when (b/get-state key)
    (p/close! (b/get-state key))
    (b/dis-attach key)))


(defn start []
  (b/start!
   (fn []
     (b/add-lifecycle start-tx-db (partial stop-db :log-db))
     (b/add-lifecycle start-schema-db (partial stop-db :schema-db))
     (b/add-lifecycle start-pieces-db (partial stop-db :pieces-db)))))
