(ns imdb.store
  {:doc "the store of pieces"})


(def store (atom {}))

(defn create-store
  [entity-name])

(defn append-piece
  [piece]
  (swap! store (fn [cur]
                 (assoc-in cur [(:entity piece) (:id piece)]  piece))))

(defn append-pieces
  [pieces]
  (map #(append-piece %) pieces))



(defn find-by-id
  [entity-name id]
  (get-in @store [entity-name id]))


(defn find-by-ids
  [entity-name ids]
  (filter (comp not empty?) (map #(find-by-id entity-name %) ids)))


(def piece-example
  {:eid 112126, :entity :user, :id 14345969392870000, :key :event, :val :change-name, :date 1212121})

(def pieces-example
  '({:eid 112126, :entity :user, :id 14345969392870000, :key :event, :val :change-name, :date 1212121}
    {:eid 112126, :entity :user, :id 14345969392890000, :key :name, :val "hello", :date 1212121}))

#_(append-piece piece-example)
#_(append-pieces pieces-example)
#_(find-by-id :user 14345969392870000)
#_(find-by-id :user 14345969392890000)
#_(find-by-ids :user #{14345969392870000 14345969392860000})
(str @store)
