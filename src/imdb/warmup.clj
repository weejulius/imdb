(ns imdb.warmup
  (:require [imdb.boot :as b]
            [imdb.store :as store]
            [imdb.protocol :as p]
            [imdb.index :as idx]
            [imdb.schema :as schema]
            [common.convert :as cvt]))

(def suffix "db12")

(defn start-tx-db
  []
  (b/attach :log-db (store/->store
                     (b/get-state :tx-db-path (str "/tmp/tx-" suffix))
                     {:key-encoder cvt/->bytes
                      :val-encoder cvt/->bytes
                      :key-decoder cvt/->long
                      :val-decoder cvt/->data} )))

(defn start-schema-db
  []
  (b/attach :schema-db (store/->store
                        (b/get-state :tx-db-path (str "/tmp/schema-" suffix))
                        {:key-encoder cvt/->bytes
                         :val-encoder cvt/->bytes
                         :key-decoder cvt/->long
                         :val-decoder cvt/->data} )))

(defn start-pieces-db
  []
  (b/attach :pieces-db (store/->store
                        (b/get-state :tx-db-path (str "/tmp/pieces-" suffix))
                        {:key-encoder cvt/->bytes
                         :val-encoder cvt/->bytes
                         :key-decoder cvt/->long
                         :val-decoder cvt/->data} )))


(defn stop-db
  [key]
  (when (b/get-state key)
    (p/close! (b/get-state key))
    (b/dis-attach key)))

(defn start-dbs
  []
  (start-schema-db)
  (start-pieces-db)
  (start-tx-db))


(defn stop-dbs
  []
  (doseq [key [:log-db :schema-db :pieces-db]]
    (stop-db key)))
