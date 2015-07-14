(ns ^{:doc "the shema for each kind of entity"}
  imdb.schema
  (:require [imdb.protocol :as p])
  (:use [clojure.test]))



;; the format of schema

(def schema-exmple
  {:name [:string :index :uniqure]
   :age [:int :index]})


(defn piece-name-id->name
  "get the corresponding name from the id"
  [piece-name-ids]
  (fn [entity-name id]
    (ffirst (filter #(= id (second %))
                    (entity-name piece-name-ids)))))


(defn piece-name->id
  "get the mapped id from the piece name"
  [piece-name-ids]
  (fn [entity-name piece-name]
    (get-in piece-name-ids
            [entity-name piece-name])))


(defn piece-name->schema-def
  [schemas]
  (fn [entity-name piece-key]
    (get-in schemas [entity-name piece-key])))


(defn piece-schema-def-validate
  [piece]
  )

(defn- piece-ids->max-id
  [piece-name-ids]
  (let [piece-ids (flatten
                   (map #(vals (second %)) piece-name-ids))]
    (if (empty? piece-ids)
      0 (apply max piece-ids))))

(defn  schema->pieces-without-id
  "find out the pieces which does not have id"
  [piece-name-ids]
  (fn [schemas]
    (let [max-id (atom (piece-ids->max-id piece-name-ids))]
      (apply concat (keep (fn [[k v]]
                            (keep (fn [[k1 v1]]
                                    (if (nil? (get-in piece-name-ids [k k1]))
                                      [k k1 (swap! max-id inc)]))
                                  v))
                          schemas)))))




(defn schema->piece-name-ids
  "populate the id for the pieces does not have and return all the id"
  [origin-piece-name-ids schema->pieces-without-id]
  (fn [schemas]
    (let [new-pieces (schema->pieces-without-id schemas)]
      (reduce #(assoc-in %1
                         [(first %2) (second %2)]
                         (nth %2 2))
              origin-piece-name-ids
              new-pieces))))

(defn pieces-name-ids->store
  "store the map between the piece name and id"
  [schema->piece-name-ids piece-name-ids->store!]
  (fn [schemas]
    (let [piece-name-ids (schema->piece-name-ids schemas)]
      (when-not (empty? piece-name-ids)
        (piece-name-ids->store! piece-name-ids))
      piece-name-ids)))


(def piece-name-ids-sample
  {:user {:name 1
          :event 2}})

(deftest test-piece-name-by-id
  (testing ""
    (is (= :name ((piece-name-id->name piece-name-ids-sample)  :user 1)))
    (is (= 2 ((piece-name->id piece-name-ids-sample)  :user :event)))))


(def schema-sample {:user {:name [:string]
                           :event [:string]
                           :age [:string]
                           :sex [:string]}
                    :hello {:name [:string]}})


(deftest test-init-schema
  (testing ""
    (is (= 3 (get-in ((schema->piece-name-ids
                       piece-name-ids-sample
                       (schema->pieces-without-id piece-name-ids-sample))
                      schema-sample)
                     [:user :age])))))
