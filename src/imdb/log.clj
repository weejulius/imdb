(ns  ^{:doc "store the cmd log and can be used to rebuild index or store"}
  imdb.log)


(def logs "the logs of cmd" [])

(defn append
  [cmd]
  (assoc logs cmd))
