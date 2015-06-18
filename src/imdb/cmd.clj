(ns  ^{:doc "cmd convertor which adapt the various cmd format to the unified"}
  imdb.cmd
  (:require [imdb.log :as log]
            [imdb.index :as idx]
            [imdb.id-creator :as idc]
            [imdb.store :as store]
            [imdb.query :as q])
  (:use [clojure.test]))


(def cmd-example
  {:entity :user
   :event :change-name
   :eid 112122
   :date 1212121
   :name "new name"})

(def what-is-piece
  {:eid 11221
   :id 121212
   :key :name
   :val "value"
   :date 12121212})

(defn gen-piece-id
  [entity-name]
  (idc/gen-id))

(defn in?
  "true if seq contains elm"
  [seq elm]
  (some #(= elm %) seq))

(defn mk-piece
  [entity-name entity-id date kv]
  (let [k (first kv)
        v (second kv)]
    (if (not (in? [:entity :date :eid] k))
      {:eid entity-id
       :entity entity-name
       :id (gen-piece-id entity-name)
       :key k
       :val v
       :date date})))

;;todo validate pieces
(defn cmd-to-pieces
  "break the cmd to pieces"
  [cmd]
  (let [entity-name (:entity cmd)
        event (:event cmd)
        entity-id (:eid cmd)
        date (:date cmd)]
    (filter (comp not empty?) (map #(mk-piece entity-name entity-id date %) cmd))))

(defn pub
  "the client api used to send cmd to server"
  [cmd]
  (if-let [pieces (cmd-to-pieces cmd)]
    (do
      (doall (map #(idx/update-index %) pieces))
      (store/append-pieces pieces))))

#_(cmd-to-pieces cmd-example)



(deftest test-cmd-to-pieces
  (testing ""
    (is (= 2 (count (cmd-to-pieces cmd-example))))))


(pub cmd-example)

(deftest test-pub
  (test ""
        (is ())))


(q/find-entity :user 112122)
(q/find-entity-by-index :user :name "new name")
(str @store/store)
(str @idx/user-kindex)
