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
               (in? [:between :and :or :asc :>= :limit :count] k) k
               (and (coll? i) (sc/piece-name-id k (second i)))  :index)]
      new)))

(defmethod search :between [[k p] r]
  (subseq r >= (idx/hash-key (first p)) <= (idx/hash-key (second p))))

(defmethod search :asc [i r]
  (reverse r))

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


(defn merge-index [idx]
  (if idx
    (reduce (fn [r i]
              (concat r (second i))) [] idx)))

(defmethod search :index [i r]
  (-> (proc-index i)
      merge-index
      filter-vindex))

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
  [m]
  (let [query (:query m)
        entity-name (:entity m)
        idx (search query nil)
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
      (is (= '(111111) (:idx (q query-sample1)))) )))

(q query-sample1)
(q query-sample)
