(ns ^{:doc "used to query the index and retrieve and merge result from store"}
  imdb.query
  (:require [imdb.index :as idx]
            [imdb.store :as store]))



(defn find-pieces-by-entity-id
  [entity-name id]
  (if-let [pieces-id (idx/val-kindex entity-name id)]
    (store/find-by-ids entity-name pieces-id)))


(defn cast-to-entity
  [entity-name entity-id pieces]
  (if (not (empty? pieces))
    (let [entity {:eid entity-id
                  :entity entity-name}]
      (reduce (fn [r k]
                (if k (-> r
                          (assoc (:key k) (:val k))
                          (assoc :date (:date k)))))
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


(find-entity :user 112121)
#_(find-entity-by-index :user :event :change-name)
