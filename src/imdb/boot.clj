(ns imdb.boot
  (:require [clojure.tools.namespace.repl :refer [refresh]]))

(def state (atom {}))
(def before-fs (atom []))
(def after-fs (atom []))

(defn- init
  []
  (alter-var-root #'state (constantly (atom {}))))


(defn attach [k v]
  (swap! state assoc k v))

(defn dis-attach [k]
  (swap! state dissoc k))

(defn before! [f]
  (swap! before-fs conj f))

(defn start! []

  (reduce (fn [r f]
            (f r))
          state
          @before-fs))

(defn get-state
  ([k] (get @state k))
  ([k d] (get @state k d)))

(defn after! [f]
  (swap! after-fs conj f))

(defn stop! []
  (if-not (empty? @state)
    (reduce (fn [r f]
              (f r))
            state
            @after-fs))
  (init))



(defn refresh!
  []
  (stop!)
  (refresh)
  (start!))
