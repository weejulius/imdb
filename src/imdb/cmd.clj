(ns  ^{:doc "cmd convertor which adapt the various cmd format to the unified"}
  imdb.cmd
  (:require [imdb.log :as log]
            [imdb.index :as idx]
            [imdb.id-creator :as idc]
            [imdb.store :as store]
            [imdb.query :as q]
            [imdb.transaction :as tx])
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

;;;

(defn gen-piece-id
  "generate id for new piece"
  [entity-name]
  (idc/gen-id-by-time))

(defn in?
  "true if seq contains elm"
  [seq elm]
  (some #(= elm %) seq))

(defn mk-piece
  "make the piece"
  [entity-name entity-id kv]
  (let [k (first kv)
        v (second kv)]
    (if (not (in? [:entity :eid] k))
      {:eid entity-id
       :entity entity-name
       :id (gen-piece-id entity-name)
       :key k
       :val v})))

;;TODO validate pieces
(defn cmd-to-pieces
  "break the cmd to pieces"
  [cmd]
  (let [entity-name (:entity cmd)
        event (:event cmd)
        entity-id (:eid cmd)]
    (filter (comp not empty?)
            (map #(mk-piece entity-name entity-id %) cmd))))

(defn pub
  "the client api used to send cmd to server"
  [cmd]
  (if-let [pieces (cmd-to-pieces cmd)]
    (tx/run-tx pieces
               (fn [pieces]
                 (do
                   (doseq [piece pieces]
                     (idx/update-index piece))
                   (store/append-pieces pieces)))) ))

(deftest test-cmd-to-pieces
  (testing ""
    (is (= 3 (count (cmd-to-pieces cmd-example))))))

(deftest test-pub
  (testing ""
    (let [eid 1121130
          cmd {:entity :user
               :event :change-name
               :eid eid
               :date 1212121
               :name "new name"}]
      (pub cmd)
      (is (= cmd (q/find-entity :user eid)))
      (is (= cmd (first (q/find-entity-by-index :user :name "new name")))))))

(defn f->
  [& fs]
  (fn [i p]
    (reduce (fn [ r f] (f r p))
            i
            fs)))

(deftest test-range-query
  (testing ""
    (let [eid 112222
          cmds  [
                 {:entity :user
                  :event :change-name
                  :eid 111113
                  :date 1212121
                  :name "cfame"}
                 {:entity :user
                  :event :change-name
                  :eid 111111
                  :date 1212121
                  :name "aname"}
                 {:entity :user
                  :event :change-name
                  :eid 111112
                  :date 1212121
                  :name "bname"}
                 ]]
      (doseq [cmd cmds]
        (pub cmd))
      (is (= 2
             (count (q/query :user :name  q/f-between :from "aname" :to "bname"))))
      (is (= '(111113 111112)
             (map #(:eid %)
                  (q/query :user :name
                           (f-> q/f-between q/f-order-by q/f-limit)
                           :from "aname" :to "cname" :asc true :start 1 :num 2))))
      (is (= '(111112 111113)
             (map #(:eid %)
                  (q/query :user :name
                           (f-> q/f-between q/f-order-by q/f-limit)
                           :from "aname" :to "cname" :start 2 :num 2)))))))

#_(pub cmd-example)
#_(q/find-entity :user 112122)
#_(q/find-entity-by-index :user :name "new name")
(str @store/store)
