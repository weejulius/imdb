(ns ^{:doc "the store of pieces"}
  imdb.store
  (:require [imdb.schema :as schema]
            [imdb.boot :as b])
  (:use [clojure.test]))

(def store (atom {}))

(defn ref-store
  [entity-name]
  store)

(defn simpfy-piece
  "throw the unnessesaries"
  [piece]
  [(schema/piece-name-id (b/get-state :piece-name-ids) (:entity piece) (:key piece))
   (:val piece)])

(defn append-piece
  [piece]
  (swap! store (fn [cur]
                 (assoc-in cur [(:entity piece) (:id piece)]
                           (simpfy-piece piece)))))

(defn append-pieces
  [pieces]
  (doseq [piece pieces]
    (append-piece piece)))



(defn find-by-id
  "find piece by id"
  [entity-name id]
  (get-in @store [entity-name id]))


(defn find-by-ids
  "find pieces by ids"
  [entity-name ids]
  (filter (comp not empty?)
          (map #(find-by-id entity-name %) ids)))


(def piece-example
  {:eid 112126, :entity :user, :id 14345969392870000, :key :event, :val :change-name, :date 1212121})

(def pieces-example
  '({:eid 112126, :entity :user, :id 14345969392870000, :key :event, :val :change-name, :date 1212121}
    {:eid 112126, :entity :user, :id 14345969392890000, :key :name, :val "hello", :date 1212121}))

(deftest test-simply-piece
  (testing ""
    (is (= [2 :change-name] (simpfy-piece piece-example)))))

#_(append-piece piece-example)
#_(append-pieces pieces-example)
#_(find-by-id :user 14345969392870000)
#_(find-by-id :user 14345969392890000)
#_(find-by-ids :user #{14345969392870000 14345969392860000})
(str @store)
