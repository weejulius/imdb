(ns   ^{:doc "build index according the schema definitions,there are two type of index,
         one is for the key, the other is for value"}
  imdb.index
  (:require [imdb.schema :as schema]
            [pandect.core :refer [crc32 crc32-file ]]
            [common.component :as cmpt]))


(defn in?
  "true if seq contains elm"
  [seq elm]
  (some #(= elm %) seq))


(def user-kindex
  "the kindex for user"
  (atom {}))

(def event-kindex
  "the kindex for user"
  (atom {}))

(defn ref-kindex
  "get the ref of k index by entity name"
  [entity-name]
  (case  entity-name
    :user user-kindex
    ))

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
        piece-name-id (schema/piece-name-id entity-name (:key piece))
        tx-id (:tx-id piece)]
    (swap! kindex (fn [cur]
                    (update-in
                     cur
                     [entity-id piece-name-id]
                     (fn [eindex item]
                       (if eindex (conj eindex item)
                           (sorted-set item)))
                     [id tx-id])))))


(def name-user-vindex
  (atom (sorted-map)))

(def event-user-vindex
  (atom (sorted-map)))


(defn ref-vindex
  [entity-name key]
  (case key
    :event event-user-vindex
    :name name-user-vindex))

(defn hash-str
  [s]
  s)

(defn insert-to-str-vindex
  "insert new str to index, the data structure is
   btree"
  [piece]
  (let [entity-name (:entity piece)
        key (:key piece)
        v (:val piece)
        idx (hash-str v)
        vindex (ref-vindex entity-name key)
        id (:id piece)
        eid (:eid piece)
        tx-id (:tx-id piece)]
    (swap! vindex (fn [cur]
                    (update-in cur [idx] conj [eid id v tx-id])))))

(defn insert-to-vindex
  [piece]
  (let [entity-name (:entity piece)
        key (:key piece)
        schema-def (schema/piece-schema-def entity-name key)]
    (if (in? schema-def :string)
      (insert-to-str-vindex piece))))


(defn update-index
  [piece]
  (do (insert-to-kindex piece)
      (insert-to-vindex piece)))



(defn val-kindex
  [entity-name entity-id]
  (get @(ref-kindex entity-name) entity-id))



(defn val-vindex
  [entity-name key val]
  (let [vindex (ref-vindex entity-name key)
        hs (hash-str val)
        idx-v (get @vindex hs)]
    idx-v))


(defn range-vindex
  [entity-name key from to]
  (let [vindex (ref-vindex entity-name key)
        hs-from (hash-str from)
        hs-to (hash-str to)
        idx-v (subseq @vindex >= hs-from <= hs-to)]
    (if idx-v
      (reduce (fn [r i]
                (concat r (second i))) [] idx-v))) )


(def piece-example
  {:eid 112128, :entity :user, :id 14345969392890000, :key :event, :val :change-name, :date 1212121})


#_(insert-to-kindex piece-example)
#_(insert-to-vindex piece-example)
#_(update-index piece-example)
#_(val-kindex :user 112128)
#_(val-vindex :user :event :change-name)
@(ref-vindex :user :event)
@(ref-kindex :user)
