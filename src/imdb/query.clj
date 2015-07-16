(ns ^{:doc "used to query the index and retrieve and merge result from store"}
  imdb.query
  (:require [imdb.index :as idx]
            [imdb.store :as store]
            [imdb.schema :as sc]
            [imdb.common :as comm :refer [in?]]
            [imdb.boot :as b]
            [imdb.transaction :as tx])
  (:use [clojure.test]
        [imdb.common]))


(defn- kindex->filter
  "the pieces id: {piece-name-id [[id tx-id] [id1 tx-id1]]}
   1. select the lastest for each piece
   2. remove the failed transaction"
  [kindex date]
  (map (fn [id-groups]
         (loop [groups (second id-groups)]
           (if-let [group (first groups)]
             (if (tx/tx-done? (second group))
               (first group)
               (recur (rest groups))))))
       kindex))

(defcurrable entity-id->pieces
  "get the pieces id from kindex and fetch them"
  [entity-name id] [entity-id->kindex ids->pieces]
  (if-let [kindex (entity-id->kindex entity-name id)]
    (ids->pieces entity-name
                 (kindex->filter kindex nil))))


(defcurrable pieces->entity
  "cast from pieces to entity"
  [entity-name entity-id pieces] [piece-id->name]
  (when-not (empty? pieces)
    (let [entity {:eid entity-id
                  :entity entity-name}]
      (reduce (fn [r k]
                (if k
                  (assoc
                   r
                   (piece-id->name entity-name (first k))
                   (second k))))
              entity pieces))))


(defcurrable entity-id->entity
  "return entity by entity id"
  [entity-name eid] [entity-id->pieces pieces->entity]
  (->> (entity-id->pieces entity-name eid)
       (pieces->entity entity-name eid)))




(defn vindex->entity-ids
  "invalidate the vindex and concat all the entity ids"
  [index]
  (filter (comp not nil?)
          (distinct  (map (fn [idx-item]
                            (let [eid (first idx-item)
                                  ok? (tx/tx-done? (nth idx-item 2))]
                              (if ok? eid)))
                          index))))


(deftest test-concat-valid-eids
  (testing ""
    (is (= '(121) (vindex->entity-ids [[121 1111 121 121]])))))

;; a simple usage of index
(defcurrable piece-value->entities
  "search the entities matched by piece value"
  [entity-name key val] [piece-value->vindex entity-id->entity]
  (let [index (piece-value->vindex  entity-name key val)
        entities-id  (vindex->entity-ids index)]
    (when-not (empty? entities-id)
      (map #(entity-id->entity entity-name %)
           entities-id))))


(defcurrable vindex->entities
  "return entities by the provided vindex"
  [entity-name vindex] [entity-id->entity]
  (when-let [entities-id  (vindex->entity-ids vindex)]
    (map #(entity-id->entity entity-name %)
         entities-id)))

(deftest test-filter-pieces
  (testing ""
    (swap! tx/tx (fn [c]
                   (assoc c 3 0)))
    (is (= '(2 5) (kindex->filter {1 [[1 3] [2 4]]
                                   2 [[4 3] [5 4]]}
                                  nil)))))




;;;; redesign the query engine
;;;; search firstly and merge the result




(defmulti search "search the index"
  (fn [i r o]
    (let [k (if (coll? i) (first i) i)
          new (cond
                (in? [:between :start-with :end-with :and :or :asc :>= :limit :count] k) k
                (and (coll? i) ((get o :piece-name->id) k (second i)))
                :index)]
      new)))

(defmethod search :between [[k p] r o]
  (subseq r >= (idx/hash-key (first p)) <= (idx/hash-key (second p))))

(defmethod search :asc [i r p]
  (reverse r))


(defmethod search :start-with [[k p] r o]
  (take-while (fn [i]
                (.startsWith ^String (first i) p)) r))

(defmethod search :end-with [[k p] r o]
  (filter (fn [i]
            (.endsWith ^String (first i) p)) r))

(defmethod search :>= [[k p] r o]
  (subseq r >= (idx/hash-key p)))


(defmethod search :limit [[k i] r o]
  (take (second i) (drop (dec (first i)) r)))

(defmethod search :count [i r o]
  (count r))




(defn proc-index
  "process the index"
  [c o]
  (let [entity-name (first c)
        k (second c)
        s (nnext c)
        idx ((get o :piece-name->vindex) entity-name k)]
    (reduce (fn [r i]
              (search i r o))
            @idx
            s)))

(defn vindex->flatten
  "flatten the index to coll from the index map"
  [idx]
  (if idx
    (reduce (fn [r i]
              (concat r (second i))) [] idx)))

(defmethod search :index [i r o]
  (-> (proc-index i o)
      vindex->flatten
      vindex->entity-ids))

(defmethod search nil [i r o])

(defmethod search :and [i r o]
  (let [c1 (second i)
        c2 (nth i 2)
        v1 (search c1 r o)
        v2 (search c2 r o)]
    (filter (fn [i] (in? v2 i)) v1)))

(defmethod search :or [i r o]
  (let [c1 (second i)
        c2 (nth i 2)
        v1 (search c1 o)
        v2 (search c2 o)]
    (concat v1 v2)))


(defcurrable query->entities
  "query the entities"
  [clause]  [vindex->entities o]
  (let [query-clause (:query clause)
        entity-name (:entity clause)
        idx (search query-clause nil o)
        cnt (search [:count] idx o)
        idx (search [:limit (:limit clause)] idx o)]
    (when idx
      {:cnt cnt
       :idx idx
       :entities (vindex->entities entity-name idx)})))
