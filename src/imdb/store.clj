(ns ^{:doc "the store of pieces"}
  imdb.store
  (:require [imdb.schema :as schema]
            [imdb.protocol :as p]
            [clj-leveldb :as cl]
            [clojure.core.async :refer [chan go >! <!]])
  (:use [clojure.test]
        [imdb.common])
  (:import [org.mapdb DBMaker DB BTreeKeySerializer]))

(defrecord LevelStore [db]
  p/IStore
  (put! [this k v]
    (when (and k v)
      (cl/put db k v)))
  (get! [this k]
    (when k (cl/get db k)))
  (iterate! [this f]
    (doseq [[k v] (cl/iterator db)]
      (f k v)))
  (close! [this]
    (.close db)))


(defrecord MapdbStore [^org.mapdb.DB db ^java.util.Map map decodes]
  p/IStore
  (put! [this k v]
    (if (and k v)
      (.put map k v)))
  (get! [this k]
    (.get map k))
  (iterate! [this f]
    (let [it ^java.util.Iterator (.iterator (.entrySet map))]
      (while (.hasNext it)
        (let [n (.next it)]
          (f (.getKey n) (.getValue n))))))
  (close! [this]
    (.close db)))


(defn ->store
  [db-path decodes]
  (LevelStore.
   (cl/create-db db-path decodes)))

(defn create-mapdb-store
  [store-name db-path decodes]
  (let [db (.make (.transactionDisable (DBMaker/fileDB (java.io.File. db-path))))
        map (.makeOrGet (doto (.treeMapCreate db (str store-name))
                          (.keySerializer BTreeKeySerializer/LONG)))
        store (MapdbStore. db map decodes)]
    store))

(deftest test-mapdbstore
  (testing ""
    (let [store (->store  "/tmp/testdb3" {1 1})]
      (p/put! store 1 {:hello 1})
      (is (= {:hello 1} (p/get! store 1)))
      (is (nil? (p/iterate! store println))))))

(defcurrable piece->simple-piece
  "throw away the unnessesaries"
  [piece] [piece-name->id]
  [(piece-name->id (:entity piece) (:key piece))
   (:val piece)])

(def store-chan (chan))

(defn pieces->store!
  [pieces]
  (doseq [piece pieces]
    (go (>! store-chan piece))))

(defcurrable simple-piece->store!
  "store the simple piece"
  [simple-piece] [store]
  (store (first simple-piece)
         simple-piece))

(defn listen-store-req
  [piece->simple-piece simple-piece->store]
  (go (while true
        (let [piece (<! store-chan)]
          (-> piece
              piece->simple-piece
              simple-piece->store)))))

(defcurrable id->piece
  "find piece by id"
  [entity-name id] [store->query]
  (store->query entity-name id))


(defcurrable ids->pieces
  "find pieces by ids"
  [entity-name ids] [id->piece]
  (keep #(id->piece entity-name %) ids))


(def piece-example
  {:eid 112126,
   :entity :user,
   :id 14345969392870000,
   :key :event,
   :val :change-name,
   :date 1212121})

(def pieces-example
  '({:eid 112126,
     :entity :user,
     :id 14345969392870000,
     :key :event,
     :val :change-name,
     :date 1212121}

    {:eid 112126,
     :entity :user,
     :id 14345969392890000,
     :key :name,
     :val "hello",
     :date 1212121}))

(deftest test-simply-piece
  (testing ""
    (is (= [2 :change-name]
           (<< piece->simple-piece
               (fn [p y] 2)
               piece-example)))))
