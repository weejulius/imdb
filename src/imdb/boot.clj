(ns imdb.boot
  (:require [clojure.tools.namespace.repl :refer [refresh]]))

(def state (atom {}))
(def before-fs (atom []))
(def after-fs (atom []))




(defn attach [k v]
  (println "attaching >>>>> " k "  " v)
  (swap! state assoc k v))

(defn dis-attach [k]
  (swap! state dissoc k))

(defn before! [f]
  (swap! before-fs conj f))

(defn add-lifecycle
  [fstart fstop]
  (swap! before-fs conj fstart)
  (swap! after-fs conj fstop))

(defn- clear
  []
  (alter-var-root #'state (constantly (atom {})))
  (alter-var-root #'after-fs (constantly (atom [])))
  (alter-var-root #'before-fs (constantly (atom []))))



(defn start! [f]
  (clear)
  (f)
  (println @before-fs)
  (println @after-fs)
  (reduce (fn [r f]
            (f state))
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
              (f state))
            state
            @after-fs)))


(defn only-refresh!
  []
  (refresh))

(defn refresh!
  [f]
  (stop!)
  (refresh)
  (start! f))
