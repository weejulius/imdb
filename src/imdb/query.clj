(ns ^{:doc "used to query the index and retrieve and merge result from store"}
  imdb.query
  (:require [imdb.index :as idx]
            [imdb.store :as store]
            [imdb.schema :as sc]
            [imdb.cmd :as c]
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
  "get the pieces id from kindex and fetch them"
  [entity-name id]
  (if-let [pieces-id (idx/val-kindex entity-name id)]
    (store/find-by-ids entity-name
                       (filter-pieces pieces-id nil))))


(defn cast-to-entity
  "cast from pieces to entity"
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




(defn concat-valid-eids
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
    (is (= '(121) (concat-valid-eids [[121 1111 121 121]])))))

;; a simple usage of index
(defn find-entity-by-index
  [entity-name key val]
  (let [index (idx/val-vindex entity-name key val)
        entities-id  (concat-valid-eids index)]
    (if-not (empty? entities-id)
      (map #(find-entity entity-name %)
           entities-id))))


(defn fetch-entities-from-index
  [index entity-name]
  (if-let [entities-id  (concat-valid-eids index)]
    (map #(find-entity entity-name %)
         entities-id)))

(deftest test-filter-pieces
  (testing ""
    (swap! tx/tx (fn [c]
                   (assoc c 3 0)))
    (is (= '(2 5) (filter-pieces {1 [[1 3] [2 4]]
                                  2 [[4 3] [5 4]]}
                                 nil)))))

(find-entity :user 112121)
#_(find-entity-by-index :user :event :change-name)




;;;; redesign the query engine
;;;; search firstly and merge the result


(defn in?
  "true if seq contains elm"
  [seq elm]
  (some #(= elm %) seq))

(defmulti search "search the index"
  (fn [i r]
    (let [k (if (coll? i) (first i) i)
          new(cond
               (in? [:between :start-with :end-with :and :or :asc :>= :limit :count] k) k
               (and (coll? i) (sc/piece-name-id k (second i)))  :index)]
      new)))

(defmethod search :between [[k p] r]
  (subseq r >= (idx/hash-key (first p)) <= (idx/hash-key (second p))))

(defmethod search :asc [i r]
  (reverse r))


(defmethod search :start-with [[k p] r]
  (take-while (fn [i]
                (.startsWith ^String (first i) p)) r))

(defmethod search :end-with [[k p] r]
  (filter (fn [i]
            (.endsWith ^String (first i) p)) r))

(defmethod search :>= [[k p] r]
  (subseq r >= (idx/hash-key p)))


(defmethod search :limit [[k i] r]
  (take (second i) (drop (dec (first i)) r)))

(defmethod search :count [i r]
  (count r))

(defn proc-index
  [c]
  (let [entity-name (first c)
        k (second c)
        s (nnext c)
        idx (idx/ref-vindex entity-name k)]
    (reduce (fn [r i]
              (search i r))
            @idx
            s)))

(defn flatten-index
  "flatten the index to coll from the index map"
  [idx]
  (if idx
    (reduce (fn [r i]
              (concat r (second i))) [] idx)))

(defmethod search :index [i r]
  (-> (proc-index i)
      flatten-index
      concat-valid-eids))

(defmethod search nil [i r])

(defmethod search :and [i r]
  (let [c1 (second i)
        c2 (nth i 2)
        v1  (search c1 r)
        v2 (search c2 r)]
    (filter (fn [i] (in? v2 i)) v1)))

(defmethod search :or [i r]
  (let [c1 (second i)
        c2 (nth i 2)
        v1 (search c1)
        v2 (search c2)]
    (concat v1 v2)))


(defn q
  "query the index"
  [m]
  (let [query-clause (:query m)
        entity-name (:entity m)
        idx (search query-clause nil)
        cnt (search [:count] idx)
        idx (search [:limit (:limit m)] idx)]
    (when idx
      {:cnt cnt
       :idx idx
       :entities (map #(find-entity entity-name %)
                      idx)})))

(def query-sample
  {:entity :user
   :query [:and
           [:user :name [:between ["aname" "cname"]] ]
           [:user :age [:>= 18]]]
   :count true
   :limit [2 3]} )

(def query-sample1
  {:entity :user
   :query [:user :name [:between ["aname" "bname"]] :asc]
   :count true
   :limit [2 3]
   })

(def query-end-with
  {:entity :user
   :query [:user :name [:end-with "name"] :asc]
   :count true
   :limit [1 2]
   })

(deftest test-query
  (testing ""
    (let [cmds  [
                 {:entity :user
                  :event :change-name
                  :eid 111119
                  :date 1212121
                  :age 12
                  :name "dname"}
                 {:entity :user
                  :event :change-name
                  :eid 111113
                  :date 1212121
                  :age 18
                  :name "cfame"}
                 {:entity :user
                  :event :change-name
                  :eid 111111
                  :age 88
                  :date 1212121
                  :name "aname"}
                 {:entity :user
                  :event :change-name
                  :age 3
                  :eid 111112
                  :date 1212121
                  :name "bname"}
                 ]]
      (doseq [cmd cmds]
        (c/pub cmd))
      (is (= '(111113) (:idx (q query-sample))))
      (is (= '(111111) (:idx (q query-sample1))))
      (is (= '(111119 111112) (:idx (q query-end-with)))))))

(q query-sample1)
(q query-sample)
(q query-end-with)
