(ns ^{:doc "the shema for each kind of entity"}
  imdb.schema
  (:require [imdb.boot :as b]
            [imdb.protocol :as p])
  (:use [clojure.test]))



;; the format of schema

(def schema-exmple
  {:name [:string :index :uniqure]
   :age [:int :index]})


(defn piece-name-by-id
  [piece-name-ids entity-name piece-name-id]
  (ffirst (filter #(= piece-name-id (second %))
                  (entity-name piece-name-ids))))


(defn piece-name-id
  [piece-name-ids entity-name piece-name]
  (get-in piece-name-ids
          [entity-name piece-name]))


(defn piece-schema-def
  [schemas entity-name piece-key]
  (get-in schemas [entity-name piece-key]))


(defn piece-schema-def-validate
  [piece]
  )

(defn-  find-new-pieces
  [schemas piece-name-ids max-id]
  (apply concat (keep (fn [[k v]]
                        (keep (fn [[k1 v1]]
                                (if (nil? (get-in piece-name-ids [k k1]))
                                  [k k1 (swap! max-id inc)]))
                              v))
                      schemas)))

(defn- max-piece-id
  [piece-name-ids]
  (let [piece-ids (flatten
                   (map #(vals (second %)) piece-name-ids))]
    (if (empty? piece-ids)
      0 (apply max piece-ids))))


(defn gen-piece-name-id
  [piece-name-ids schemas]
  (let [max-id (atom (max-piece-id piece-name-ids))
        new-pieces (find-new-pieces schemas piece-name-ids max-id)]
    (println "pieces:" new-pieces)
    (println "schema:" schemas  piece-name-ids max-id)
    (reduce #(assoc-in %1
                       [(first %2) (second %2)]
                       (nth %2 2))
            piece-name-ids new-pieces)))

(defn store-piece-name-ids
  [schemas]
  (let [key 10000
        db (b/get-state :schema-db)
        piece-name-ids (get db key)
        piece-name-ids (gen-piece-name-id piece-name-ids schemas)]
    (when-not (empty? piece-name-ids)
      (p/put! db key piece-name-ids))
    piece-name-ids))


(def piece-name-ids-sample
  {:user {:name 1
          :event 2}})

(deftest test-piece-name-by-id
  (testing ""
    (is (= :name (piece-name-by-id piece-name-ids-sample :user 1)))
    (is (= 2 (piece-name-id piece-name-ids-sample :user :event)))))


(def schema-sample {:user {:name [:string]
                           :event [:string]
                           :age [:string]
                           :sex [:string]}
                    :hello {:name [:string]}})

(find-new-pieces schema-sample piece-name-ids-sample (atom 0))
(find-new-pieces schema-sample nil (atom 0))
(max-piece-id piece-name-ids-sample)

(deftest test-init-schema
  (testing ""
    (is (= 3 (get-in (gen-piece-name-id piece-name-ids-sample schema-sample) [:user :age])))))
