(ns imdb.core
  (:require [imdb.transaction :as tx]
            [imdb.store :as store]
            [imdb.index :as idx]
            [imdb.cmd :as cmd]
            [imdb.boot :as b]
            [imdb.schema :as sc]
            [imdb.query :as query]
            [imdb.protocol :as p]
            [imdb.warmup :as warmup])
  (:use [clojure.test]
        [imdb.common]))


(defprotocol Imdb
  "the imdb interface"
  (pub [this cmd] "publish cmd to the db")
  (q [this clause] "query from db according to clause")
  (start! [this] "start the db")
  (stop! [this] "stop the db"))



(defn pieces->handle!
  [pieces]
  (do
    (doseq [piece pieces]
      (idx/piece->make-index! piece))
    (store/pieces->store! pieces)))

(cur log-tx tx/log-tx (b/get-state :log-db))
(cur log-tx-done tx/log-tx-done (b/get-state :log-db))
(cur mark-tx tx/mark-tx log-tx)
(cur unmark-tx tx/unmark-tx log-tx-done)
(cur run-tx tx/run-tx mark-tx unmark-tx)

(defn cmd->pub
  "the client api used to send cmd to server"
  [cmd]
  (when-let [pieces (cmd/cmd-to-pieces cmd)]
    (run-tx
     pieces
     pieces->handle!) ))

(defn replay!
  [log-db]
  (p/iterate! log-db
              (fn [k v] (pieces->handle! v))))




(defn init-kindex
  [entity-name]
  (let [kidx (atom {})]
    (b/attach (str entity-name "-kidx") kidx)
    kidx))

(defn init-vindex
  [entity-name k]
  (let [vidx (atom (sorted-map))]
    (b/attach (str entity-name "-" k "-vidx") vidx)
    vidx))

(defn ref-kindex
  "get the ref of k index by entity name"
  [entity-name]
  (let [ref (b/get-state  (str entity-name "-kidx"))]
    (if ref ref (init-kindex entity-name))))

(defn ref-vindex
  "get the ref of v index by entity name and key"
  [entity-name key]
  (let [ref (b/get-state  (str entity-name "-" key "-vidx"))]
    (if ref ref (init-vindex entity-name key))))

(cur piece-name->id sc/piece-name->id (b/get-state :piece-name-ids))
(cur id->piece store/id->piece (fn [entity-name id]
                                 (p/get! (b/get-state :pieces-db) id)))

(cur ids->pieces store/ids->pieces id->piece)
(cur entity-id->kindex idx/entity-id->kindex ref-kindex)
(cur entity-id->pieces query/entity-id->pieces entity-id->kindex ids->pieces)
(cur pieces->entity query/pieces->entity piece-name->id)
(cur entity-id->entity query/entity-id->entity entity-id->pieces pieces->entity)
(cur vindex->entities query/vindex->entities entity-id->entity)
(cur query->entities query/query->entities vindex->entities
     {:piece-name->id piece-name->id
      :piece-name->vindex ref-vindex})


(defn qq
  [clause]
  (query->entities clause))


(defn piece-name-ids
  [schemas schema-db origin-ids]
  (cur schema->pieces-without-id sc/schema->pieces-without-id origin-ids)
  (cur schema->piece-name-ids sc/schema->piece-name-ids origin-ids schema->pieces-without-id)
  (cur pieces-name-ids->store
       sc/pieces-name-ids->store
       schema->piece-name-ids
       (fn [v]
         (p/put! schema-db 10000  v)))
  (pieces-name-ids->store schemas))



(defn open-listeners
  [schemas]
  (cur piece->insert-to-vindex idx/piece->insert-to-vindex ref-vindex)
  (cur piece-name->schema-def sc/piece-name->schema-def ref-vindex)
  (cur piece->try-insert-to-vindex idx/piece->try-insert-to-vindex
       piece-name->schema-def
       piece->insert-to-vindex)
  (cur piece->insert-to-kindex idx/piece->insert-to-kindex ref-kindex piece-name->id)
  (cur piece->simple-piece store/piece->simple-piece piece-name->id)
  (cur simple-piece->store! store/simple-piece->store! (fn [k v]
                                                         (p/put! (b/get-state :pieces-db)
                                                                 k
                                                                 v)))
  (idx/listen-vindex-req piece->try-insert-to-vindex)
  (idx/listen-kindex-req piece->insert-to-kindex)
  (store/listen-store-req piece->simple-piece simple-piece->store!))

(defrecord SimpleImdb [schemas]
  Imdb
  (pub [this cmd]
    (cmd->pub cmd))
  (q [this clause]
    (qq clause))
  (start! [this]
    (warmup/start-dbs)
    (open-listeners schemas)
    (b/attach :schemas schemas)
    (let [schema-db (b/get-state :schema-db)
          origin-ids (p/get! schema-db 10000)]
      (b/attach :piece-name-ids
                (piece-name-ids schemas schema-db origin-ids)))
    (replay! (b/get-state :log-db))
    (println "starting imdb"))
  (stop! [this]
    (warmup/stop-dbs)
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




(defn ->create-imdb
  [schemas]
  (SimpleImdb. schemas))

(def schemas
  {:user {:name [:string :index :uniqure]
          :event [:string :index]
          :age [:int :index]
          :date [:int :index]}
   :product {:title [:string :index]}})

(deftest test-query
  (testing ""
    (let [cmds  test-data
          imdb (SimpleImdb. schemas)]
      (start! imdb)
      (b/clear-index)
      (doseq [cmd cmds]
        (pub imdb cmd))
      (Thread/sleep 200)
      (is (= '(111113) (:idx (q imdb query-sample))))
      (is (= '(111111) (:idx (q imdb query-sample1))))
      (is (= '(111119 111112) (:idx (q imdb query-end-with))))
      (stop! imdb))))


(deftest test-replay
  (testing ""
    (let [cmds  test-data
          imdb (SimpleImdb. schemas)]
      (b/clear-index)
      (is (nil? (b/get-state ":user-:name-vidx")))
      (start! imdb)
      (b/clear-index)
      (doseq [cmd cmds]
        (pub imdb cmd))
      (Thread/sleep 200)
      (is (not (nil? (b/get-state ":user-:name-vidx"))))
      (stop! imdb)
      (start! imdb)
      (not (nil?  (:idx (q imdb query-sample))))
      (not (nil? (:idx (q imdb query-sample1))))
      (not (nil? (:idx (q imdb query-end-with))))
      (stop! imdb))))
