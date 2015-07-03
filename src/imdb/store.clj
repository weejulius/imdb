(ns ^{:doc "the store of pieces"}
  imdb.store
  (:require [imdb.schema :as schema]
            [imdb.boot :as b]
            [imdb.protocol :as p]
            [clj-leveldb :as cl]
            [clojure.core.async :refer [chan go >! <!]])
  (:use [clojure.test])
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


(defn create-store
  [store-name db-path decodes]
  (b/attach store-name (LevelStore. (cl/create-db db-path
                                                  decodes))))

(defn create-mapdb-store
  [store-name db-path decodes]
  (let [db (.make (.transactionDisable (DBMaker/fileDB (java.io.File. db-path))))
        map (.makeOrGet (doto (.treeMapCreate db (str store-name))
                          (.keySerializer BTreeKeySerializer/LONG)))
        store (MapdbStore. db map decodes)]
    (b/attach store-name store)
    store))

(deftest test-mapdbstore
  (testing ""
    (let [store (create-store :test "/tmp/testdb3" {1 1})]
      (p/put! store 1 {:hello 1})
      (is (= {:hello 1} (p/get! store 1)))
      (is (nil? (p/iterate! store println))))))



(defn ref-store
  [entity-name]
  (b/get-state :pieces-db))

(defn simpfy-piece
  "throw away the unnessesaries"
  [piece]
  [(schema/piece-name-id
    (b/get-state :piece-name-ids)
    (:entity piece)
    (:key piece))
   (:val piece)])

(defn append-piece
  [piece]
  (p/put! (ref-store (:entity piece))
          (:id piece)
          (simpfy-piece piece)))


(def store-chan (chan))

(defn append-pieces
  [pieces]
  (doseq [piece pieces]
    (go (>! store-chan piece))))

(defn listen-store-req
  []
  (go (while true
        (let [piece (<! store-chan)]
          (append-piece piece)))))

(defn find-by-id
  "find piece by id"
  [entity-name id]
  (p/get! (ref-store entity-name) id))


(defn find-by-ids
  "find pieces by ids"
  [entity-name ids]
  (keep #(find-by-id entity-name %) ids))


(def piece-example
  {:eid 112126, :entity :user, :id 14345969392870000, :key :event, :val :change-name, :date 1212121})

(def pieces-example
  '({:eid 112126, :entity :user, :id 14345969392870000, :key :event, :val :change-name, :date 1212121}
    {:eid 112126, :entity :user, :id 14345969392890000, :key :name, :val "hello", :date 1212121}))

(deftest test-simply-piece
  (testing ""
    (is (= [2 :change-name] (simpfy-piece piece-example)))))

#_(append-piece piece-example)
#_(append-pieces pieces-example)
#_(find-by-id :user 14345969392870000)
#_(find-by-id :user 14345969392890000)
#_(find-by-ids :user #{14345969392870000 14345969392860000})
