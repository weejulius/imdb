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
  (if-let [pieces-id (idx/val-kindex entity-name id)]
    (store/find-by-ids entity-name
                       (filter-pieces pieces-id nil))))


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


(defn filter-vindex
  [index]
  (filter (comp not nil?)
          (distinct  (map (fn [idx-item]
                       (let [eid (first idx-item)
                             ok? (tx/tx-done? (nth idx-item 2))]
                         (if ok? eid)))
                     index))))


(deftest test-filter-vindex
  (testing ""
    (is (= '(121) (filter-vindex [[121 1111 121 121]])))))

;; a simple usage of index
(defn find-entity-by-index
  [entity-name key val]
  (let [index (idx/val-vindex entity-name key val)
        entities-id  (filter-vindex index)]
    (if-not (empty? entities-id)
      (map #(find-entity entity-name %)
           entities-id))))

(defn f-between
  [index {from :from to :to}]
  (subseq index >= (idx/hash-key from) <= (idx/hash-key to)))


(defn f-order-by
  [index {asc :asc}]
  (if asc
    (reverse index)
    index))

(defn f-limit
  [index {start :start num :num}]
  (take num (drop (dec start) index)))

;index (idx/func-vindex entity-name key between from to)

(defn fetch-entities-from-index
  [index entity-name]
  (if-let [entities-id  (filter-vindex index)]
    (map #(find-entity entity-name %)
         entities-id)))

(defn query
  [entity-name key f & {:as params}]
  (-> (idx/func-vindex entity-name key #(f % params))
      (fetch-entities-from-index entity-name)))


(deftest test-filter-pieces
  (testing ""
    (swap! tx/tx (fn [c]
                   (assoc c 3 0)))
    (is (= '(2 5) (filter-pieces {1 [[1 3] [2 4]]
                                  2 [[4 3] [5 4]]}
                                 nil)))))

(find-entity :user 112121)
#_(find-entity-by-index :user :event :change-name)
