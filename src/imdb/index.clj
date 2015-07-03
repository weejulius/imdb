(ns   ^{:doc "build index according the schema definitions,there are two type of index,
         one is for the key, the other is for value"}
  imdb.index
  (:require [imdb.schema :as schema]
            [pandect.core :refer [crc32 crc32-file ]]
            [common.component :as cmpt]
            [imdb.boot :as b]
            [clojure.core.async :refer [chan >! go <!]]))


(defn in?
  "true if seq contains elm"
  [seq elm]
  (some #(= elm %) seq))


(defn init-kindex
  [entity-name]
  (let [kidx (atom {})]
    (b/attach (str entity-name "-kidx") kidx)
    kidx))

(defn init-vindex
  [entity-name k]
  (let [vidx (atom (sorted-map))]
    (b/attach (str entity-name "-" k "-vidx") vidx)
    vidx))

(defn ref-kindex
  "get the ref of k index by entity name"
  [entity-name]
  (let [ref (b/get-state  (str entity-name "-kidx"))]
    (if ref ref (init-kindex entity-name))))

(defn ref-vindex
  "get the ref of v index by entity name and key"
  [entity-name key]
  (let [ref (b/get-state  (str entity-name "-" key "-vidx"))]
    (if ref ref (init-vindex entity-name key))))






(defn hash-key
  [s]
  (if (keyword? s) (str s)
      s))

(defn insert-item-to-vindex
  "insert new item to index"
  [piece]
  (let [entity-name (:entity piece)
        key (:key piece)
        v (:val piece)
        idx (hash-key v)
        vindex (ref-vindex entity-name key)
        id (:id piece)
        eid (:eid piece)
        tx-id (:tx-id piece)]
    (if vindex
      (swap! vindex (fn [cur]
                      (update-in cur [idx] conj [eid id tx-id]))))))

(defn insert-to-kindex
  "insert piece to k index, the data structure is
   { eid { piece-name-id [[id  tx-id] ..]
           piece-name-id1 [[id1 tx-id1] ..]
         } "
  [piece]
  (let [entity-name (:entity piece)
        entity-id (:eid piece)
        kindex (ref-kindex entity-name)
        id (:id piece)
        piece-name-id (schema/piece-name-id (b/get-state :piece-name-ids) entity-name (:key piece))
        tx-id (:tx-id piece)]
    (swap! kindex (fn [cur]
                    (update-in
                     cur
                     [entity-id piece-name-id]
                     (fn [eindex item]
                       (if eindex (conj eindex item)
                           (sorted-set item)))
                     [id tx-id])))))

(defn insert-to-vindex
  [piece]
  (let [entity-name (:entity piece)
        key (:key piece)
        schema-def (schema/piece-schema-def (b/get-state :schemas) entity-name key)]
    (if schema-def
      (insert-item-to-vindex piece))))


(def vindex-chan (chan))
(def kindex-chan (chan))

(defn update-index
  [piece]
  (go (>! kindex-chan piece)
      (>! vindex-chan piece)))

(defn listen-vindex-req
  []
  (go (while true
        (let [piece (<! vindex-chan)]
          (insert-to-vindex piece)))))

(defn listen-kindex-req
  []
  (go (while true
        (let [piece (<! kindex-chan)]
          (insert-to-kindex piece)))))

(defn val-kindex
  [entity-name entity-id]
  (get @(ref-kindex entity-name) entity-id))



(defn val-vindex
  [entity-name key val]
  (let [vindex (ref-vindex entity-name key)
        hs (hash-key val)
        idx-v (get @vindex hs)]
    idx-v))



(def piece-example
  {:eid 112128, :entity :user, :id 14345969392890000, :key :event, :val :change-name, :date 1212121})


#_(insert-to-kindex piece-example)
#_(insert-to-vindex piece-example)
#_(update-index piece-example)
#_(val-kindex :user 112128)
#_(val-vindex :user :event :change-name)
#_(ref-vindex :user :name)
#_(ref-kindex :user)
