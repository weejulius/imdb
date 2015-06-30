(ns imdb.core
  (:require [imdb.transaction :as tx]
            [imdb.store :as store]
            [imdb.index :as idx]
            [imdb.cmd :as cmd]
            [imdb.boot :as b]
            [imdb.schema :as sc]
            [imdb.query :as query]
            [imdb.protocol :as p]
            )
  (:use [clojure.test]))


(defprotocol Imdb
  "the imdb interface"
  (pub [this cmd] "publish cmd to the db")
  (q [this clause] "query from db according to clause")
  (start! [this] "start the db")
  (stop! [this] "stop the db"))


(defn handle-pieces
  [pieces]
  (do
    (doseq [piece pieces]
      (idx/update-index piece))
    (store/append-pieces pieces)))


(defn publish-cmd
  "the client api used to send cmd to server"
  [cmd]
  (if-let [pieces (cmd/cmd-to-pieces cmd)]
    (tx/run-tx pieces handle-pieces) ))

(defn replay!
  []
  (p/iterate! (b/get-state :log-db)
              (fn [k v] (handle-pieces v))))

(def schemas
  {:user {:name [:string :index :uniqure]
          :event [:string :index]
          :age [:int :index]}
   :product {:title [:string :index]}})

(defrecord SimpleImdb [schemas]
  Imdb
  (pub [this cmd]
    (publish-cmd cmd))
  (q [this clause]
    (query/query-index clause))
  (start! [this]
    (b/attach :schemas schemas)
    (b/attach :piece-name-ids (sc/store-piece-name-ids schemas))
    (replay!)
    (println "starting imdb"))
  (stop! [this]
    (println "stopping imdb")))






(def query-sample
  {:entity :user
   :query [:and
           [:user :name [:between ["aname" "cname"]] ]
           [:user :age [:>= 18]]]
   :count true
   :limit [2 3]} )

(def query-sample1
  {:entity :user
   :query [:user :name [:between ["aname" "bname"]] :asc]
   :count true
   :limit [2 3]
   })


(def query-end-with
  {:entity :user
   :query [:user :name [:end-with "name"] :asc]
   :count true
   :limit [1 2]
   })


(def test-data [
                {:entity :user
                 :event :change-name
                 :eid 111119
                 :date 1212121
                 :age 12
                 :name "dname"}
                {:entity :user
                 :event :change-name
                 :eid 111113
                 :date 1212121
                 :age 18
                 :name "cfame"}
                {:entity :user
                 :event :change-name
                 :eid 111111
                 :age 88
                 :date 1212121
                 :name "aname"}
                {:entity :user
                 :event :change-name
                 :age 3
                 :eid 111112
                 :date 1212121
                 :name "bname"}])

(deftest test-query
  (testing ""
    (let [cmds  test-data
          imdb (SimpleImdb. schemas)]
      (start! imdb)
      (doseq [cmd cmds]
        (pub imdb cmd))
      (is (= '(111113) (:idx (q imdb query-sample))))
      (is (= '(111111) (:idx (q imdb query-sample1))))
      (is (= '(111119 111112) (:idx (q imdb query-end-with))))
      (stop! imdb))))


(deftest test-replay
  (testing ""
    (let [cmds  test-data
          imdb (SimpleImdb. schemas)]
      (start! imdb)
      (doseq [cmd cmds]
        (pub imdb cmd))
      (is (not (nil? (b/get-state ":user-:name-vidx"))))
      (b/clear-index)
      (is (nil? (b/get-state ":user-:name-vidx")))
      (stop! imdb)
      (start! imdb)
      (is (= '(111113) (:idx (q imdb query-sample))))
      (is (= '(111111) (:idx (q imdb query-sample1))))
      (is (= '(111119 111112) (:idx (q imdb query-end-with))))
      (stop! imdb))))
