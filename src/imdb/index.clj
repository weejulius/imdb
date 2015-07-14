(ns   ^{:doc "build index according the schema definitions,there are two type of index,
         one is for the key, the other is for value"}
  imdb.index
  (:require [imdb.schema :as schema]
            [pandect.core :refer [crc32 crc32-file ]]
            [common.component :as cmpt]
            [clojure.core.async :refer [chan >! go <!]]))

(defn hash-key
  [s]
  (if (keyword? s) (str s)
      s))

(defn piece->insert-to-vindex
  "insert new item to index"
  [piece-name->ref-vindex]
  (fn [piece]
    (let [entity-name (:entity piece)
          piece-name (:key piece)
          v (:val piece)
          hash-val (hash-key v)
          vindex (piece-name->ref-vindex entity-name piece-name)
          id (:id piece)
          eid (:eid piece)
          tx-id (:tx-id piece)]
      (if vindex
        (swap! vindex (fn [cur]
                        (update-in cur
                                   [hash-val]
                                   conj
                                   [eid id tx-id])))))))

(defn piece->insert-to-kindex
  "insert piece to k index, the data structure is
   { eid { piece-name-id [[id  tx-id] ..]
           piece-name-id1 [[id1 tx-id1] ..]
         }}"
  [entity->ref-kindex piece-name->id]
  (fn [piece]
    (let [entity-name (:entity piece)
          entity-id (:eid piece)
          kindex (entity->ref-kindex entity-name)
          id (:id piece)
          piece-name-id (piece-name->id entity-name (:key piece))
          tx-id (:tx-id piece)]
      (swap! kindex (fn [cur]
                      (update-in
                       cur
                       [entity-id piece-name-id]
                       (fn [eindex item]
                         (if eindex (conj eindex item)
                             (sorted-set item)))
                       [id tx-id])))) ))

(defn piece->try-insert-to-vindex
  [piece-name->schema-def piece->insert-to-vindex]
  (fn [piece]
    (let [entity-name (:entity piece)
          key (:key piece)
          has-vindex? (not (piece-name->schema-def entity-name key))]
      (if has-vindex?
        (piece->insert-to-vindex piece))) ))


(def vindex-chan (chan))
(def kindex-chan (chan))

(defn piece->make-index!
  [piece]
  (go (>! kindex-chan piece)
      (>! vindex-chan piece)))

(defn listen-vindex-req
  [piece->try-insert-to-vindex]
  (go (while true
        (let [piece (<! vindex-chan)]
          (piece->try-insert-to-vindex piece)))))

(defn listen-kindex-req
  [piece->insert-to-kindex]
  (go (while true
        (let [piece (<! kindex-chan)]
          (piece->insert-to-kindex piece)))))

(defn entity-id->kindex
  [entity->ref-kindex]
  (fn [entity-name entity-id]
    (get @(entity->ref-kindex entity-name) entity-id)))



(defn piece-value->vindex
  "return the vindex assoc with the piece value"
  [piece-name->ref-vindex]
  (fn [entity-name key val]
    (let [vindex (piece-name->ref-vindex entity-name key)
          hs (hash-key val)
          idx-v (get @vindex hs)]
      idx-v)))



(def piece-example
  {:eid 112128, :entity :user, :id 14345969392890000, :key :event, :val :change-name, :date 1212121})
