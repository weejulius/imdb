(ns ^{:doc "the shema for each kind of entity"}
  imdb.schema
  (:require [imdb.protocol :as p])
  (:use [clojure.test]
        [imdb.common]))



;; the format of schema

(def schema-exmple
  {:name [:string :index :uniqure]
   :age [:int :index]})


(defcurrable piece-name-id->name
  "get the corresponding name from the id"
  [entity-name id] [piece-name-ids]
  (->> (entity-name piece-name-ids)
       (filter #(= id (second %)))
       ffirst))


(defcurrable piece-name->id
  "get the mapped id from the piece name"
  [entity-name piece-name] [piece-name-ids]
  (-> (piece-name-ids)
      (get-in [entity-name piece-name])))


(defcurrable piece-name->schema-def
  "get the schema def for the piece name"
  [entity-name piece-key] [schemas]
  (get-in schemas
          [entity-name piece-key]))


(defn piece-schema-def-validate
  [piece]
  )

(defn- piece-ids->max-id
  [piece-name-ids]
  (let [piece-ids (flatten
                   (map #(vals (second %)) piece-name-ids))]
    (if (empty? piece-ids)
      0 (apply max piece-ids))))

(defcurrable  schema->pieces-without-id
  "find out the pieces which does not have id"
  [schemas] [get-piece-name-ids]
  (let [piece-name-ids (get-piece-name-ids)
        max-id (atom (piece-ids->max-id piece-name-ids))]
    (apply concat (keep (fn [[k v]]
                          (keep (fn [[k1 v1]]
                                  (if (nil? (get-in piece-name-ids [k k1]))
                                    [k k1 (swap! max-id inc)]))
                                v))
                        schemas))))




(defcurrable schema->piece-name-ids
  "populate the id for the pieces does not have and return all the id"
  [schemas] [get-origin-piece-name-ids schema->pieces-without-id]
  (let [new-pieces (schema->pieces-without-id schemas)]
    (reduce #(assoc-in %1
                       [(first %2) (second %2)]
                       (nth %2 2))
            (get-origin-piece-name-ids)
            new-pieces)))

(defcurrable pieces-name-ids->store
  "store the map between the piece name and id"
  [schemas] [schema->piece-name-ids piece-name-ids->store!]
  (some-> schemas
          schema->piece-name-ids
          piece-name-ids->store!))


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
