(ns ^{:doc "the shema for each kind of entity"}
  imdb.schema
  (:use [clojure.test]))



;; the format of schema

(def schema-exmple
  {:name [:string :index :uniqure]
   :age [:int :index]})

(def piece-name-map
  (atom {:user {:name 1
                :event 2
                :date 3
                :age 4}}))

(defn piece-name-by-id
  [entity-name piece-name-id]
  (ffirst (filter #(= piece-name-id (second %))
                  (entity-name @piece-name-map))))


(defn piece-name-id
  [entity-name piece-name]
  (get-in @piece-name-map [entity-name piece-name]))

(def schemas
  (atom {:user {:name [:string :index :uniqure]
                :event [:string :index]
                :age [:int :index]}
         :product {:title [:string :index]}}))


(defn piece-schema-def
  [entity-name piece-key]
  (get-in @schemas [entity-name piece-key]))


(defn piece-schema-def-validate
  [piece]
  )


(deftest test-piece-name-by-id
  (testing ""
    (is (= :name (piece-name-by-id :user 1)))
    (is (= 3 (piece-name-id :user :event)))))
