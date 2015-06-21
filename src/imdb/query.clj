(ns ^{:doc "used to query the index and retrieve and merge result from store"}
  imdb.query
  (:require [imdb.index :as idx]
            [imdb.store :as store]
            [imdb.schema :as sc]
            [imdb.transaction :as tx])
  (:use [clojure.test]))


(defn filter-pieces
  "the pieces id: {pid [[id tx-id] [id1 tx-id1]]}
   1. select the lastest for each piece
   2. remove the failed transaction"
  [pieces-id date]
  (map (fn [id-groups]
         (loop [groups (second id-groups)]
           (if-let [group (first groups)]
             (if (tx/tx-done? (second group))
               (first group)
               (recur (rest groups))))))
       pieces-id))

(defn find-pieces-by-entity-id
  [entity-name id]
  (if-let [pieces-id (idx/val-kindex entity-name id)
           pieces-id (filter-pieces pieces-id)]
    (store/find-by-ids entity-name pieces-id)))


(defn cast-to-entity
  [entity-name entity-id pieces]
  (if (not (empty? pieces))
    (let [entity {:eid entity-id
                  :entity entity-name}]
      (reduce (fn [r k]
                (if k (-> r
                          (assoc
                           (sc/piece-name-by-id entity-name (first k))
                           (second k)))))
              entity pieces))))


(defn find-entity
  [entity-name eid]
  (cast-to-entity entity-name eid
                  (find-pieces-by-entity-id entity-name eid)))


;; a simple usage of index
(defn find-entity-by-index
  [entity-name key val]
  (let [entities-id (idx/val-vindex entity-name key val)]
    (map #(find-entity entity-name %) entities-id)))


(deftest test-filter-pieces
  (testing ""
    (swap! tx/tx (fn [c]
                   (assoc c 3 0)))
    (is (= '(2 5) (filter-pieces {1 [[1 3] [2 4]]
                                  2 [[4 3] [5 4]]}
                                 nil)))))

(find-entity :user 112121)
#_(find-entity-by-index :user :event :change-name)
