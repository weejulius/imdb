(ns imdb.perf
  (:require [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [imdb.cmd :as cmd]
            [imdb.query :as q]
            [imdb.index :as idx]))


(def pub-cmds
  (prop/for-all [event gen/string
                 eid gen/int
                 age gen/int
                 name gen/string]
                (cmd/pub {:entity :user
                          :event event
                          :age age
                          :eid eid
                          :date 1212
                          :name name})))

#_(time (tc/quick-check 10000 pub-cmds))

(str (count @idx/name-user-vindex) )

(def query
  (prop/for-all [eid gen/int]
                (or (q/find-entity :user eid) true) ))

(time (tc/quick-check 100000 query))


(q/find-entity :user 195)


(def query-by-name
  (prop/for-all [name gen/string]
                (or (q/find-entity-by-index :user :name name) true)))


#_(time (tc/quick-check 13000 query-by-name))


(def query-by-age
  (prop/for-all [age gen/int]
                (or (q/find-entity-by-index :user :age age) true)))

#_(time (tc/quick-check 13000 query-by-age))

(q/find-entity-by-index :user :age 2)