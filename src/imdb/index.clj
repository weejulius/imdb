(ns   ^{:doc "build index according the schema definitions,there are two type of index,
         one is for the key, the other is for value"}
  imdb.index
  (:require [imdb.schema :as schema]
            [pandect.core :refer [crc32 crc32-file ]]
            [common.component :as cmpt]
            [clojure.core.async :refer [chan >! go <!]])
  (:use [imdb.common]))

(defn hash-key
  [s]
  (if (keyword? s) (str s)
      s))

(defcurrable piece->insert-to-vindex
  "insert new item to index"
  [piece] [piece-name->ref-vindex]
  (let [entity-name (:entity piece)
        piece-name (:key piece)
        v (:val piece)
        hash-val (hash-key v)
        id (:id piece)
        eid (:eid piece)
        tx-id (:tx-id piece)]
    (some-> (piece-name->ref-vindex entity-name piece-name)
            (swap! (fn [cur]
                     (update-in cur
                                [hash-val]
                                conj
                                [eid id tx-id]))))))

(defcurrable piece->insert-to-kindex
  "insert piece to k index, the data structure is
   { eid { piece-name-id [[id  tx-id] ..]
           piece-name-id1 [[id1 tx-id1] ..]
         }}"
  [piece] [entity->ref-kindex piece-name->id]
  (let [entity-name (:entity piece)
        entity-id (:eid piece)
        id (:id piece)
        piece-name-id (piece-name->id entity-name (:key piece))
        tx-id (:tx-id piece)]
    (some-> (entity->ref-kindex entity-name)
            (swap! (fn [cur]
                     (update-in
                      cur
                      [entity-id piece-name-id]
                      (fn [eindex item]
                        (if eindex (conj eindex item)
                            (sorted-set item)))
                      [id tx-id]))))))

(defcurrable piece->try-insert-to-vindex
  "try to insert the piece to vindex"
  [piece] [piece-name->schema-def piece->insert-to-vindex]
  (let [entity-name (:entity piece)
        key (:key piece)
        has-vindex? (not (piece-name->schema-def entity-name key))]
    (if has-vindex?
      (piece->insert-to-vindex piece))))


(def vindex-chan (chan))
(def kindex-chan (chan))

(defn piece->make-index!
  [piece]
  (go (>! kindex-chan piece)
      (>! vindex-chan piece)))

(defn listen-vindex-req
  [piece->try-insert-to-vindex]
  (go (while true
        (some-> (<! vindex-chan)
                piece->try-insert-to-vindex))))

(defn listen-kindex-req
  [piece->insert-to-kindex]
  (go (while true
        (some-> (<! kindex-chan)
                piece->insert-to-kindex))))

(defcurrable entity-id->kindex
  "return kindex for the entity id"
  [entity-name entity-id] [entity->ref-kindex]
  (some-> @(entity->ref-kindex entity-name)
          (get entity-id)))



(defcurrable piece-value->vindex
  "return the vindex assoc with the piece value"
  [entity-name key val] [piece-name->ref-vindex]
  (some-> @(piece-name->ref-vindex entity-name key)
          (get (hash-key val))))



(def piece-example
  {:eid 112128, :entity :user, :id 14345969392890000, :key :event, :val :change-name, :date 1212121})
