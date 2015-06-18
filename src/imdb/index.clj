(ns   ^{:doc "build index according the schema definitions,there are two type of index,
         one is for the key, the other is for value"}
  imdb.index
  (:require [imdb.schema :as schema]
            [pandect.core :refer [crc32 crc32-file]]))


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
  "insert piece to k index"
  [piece]
  (let [entity-name (:entity piece)
        entity-id (:eid piece)
        kindex (ref-kindex entity-name)
        id (:id piece)]
    (swap! kindex (fn [cur]
                    (update-in
                     cur
                     [entity-id]
                     (fn [eindex id]
                       (if eindex (conj eindex id)
                           (sorted-set id)))
                     id)))))


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
  (if s (crc32 (str s))))

(defn insert-to-str-vindex
  [piece]
  (let [entity-name (:entity piece)
        key (:key piece)
        v (:val piece)
        idx (hash-str v)
        vindex (ref-vindex entity-name key)
        id (:id piece)
        eid (:eid piece)]
    (swap! vindex (fn [cur]
                    (update-in cur [idx] conj [eid id v])))))

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
    (set (map #(first %) idx-v))))

(def piece-example
  {:eid 112128, :entity :user, :id 14345969392890000, :key :event, :val :change-name, :date 1212121})

#_(insert-to-kindex piece-example)
#_(insert-to-vindex piece-example)
#_(update-index piece-example)
#_(val-kindex :user 112128)
#_(val-vindex :user :event :change-name)
@(ref-vindex :user :event)
@(ref-kindex :user)
