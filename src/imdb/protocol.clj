(ns imdb.protocol
  (:refer-clojure :exclude [get iterate]))

(defprotocol IStore
  "storage"
  (put! [this k v] "put key and value")
  (get! [this k] "get the value by key")
  (iterate! [this f] "apply f to all the entries")
  (close! [this] "close the storage"))
