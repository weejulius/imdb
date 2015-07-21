(ns imdb.boot
  (:require [clojure.tools.namespace.repl :refer [refresh]]))

(def state (atom {}))
(def before-fs (atom []))
(def after-fs (atom []))




(defn attach [k v]
  (println ">>>>>>>>>>>>>>>>>>>>>>>>>> " k )
  (swap! state assoc k v))

(defn dis-attach [k]
  (println "<<<<<<<<<<<<<<<<<<<<<<<<<<< " k)
  (swap! state dissoc k))

(defn before! [f]
  (swap! before-fs conj f))

(defn add-lifecycle
  [fstart fstop]
  (swap! before-fs conj fstart)
  (swap! after-fs conj fstop))


(defn clear-state
  []
  (println "**************** state ****************")
  (alter-var-root #'state (constantly (atom {}))))

(defn clear-index
  []
  (println "**************** index *****************")
  (doseq [c @state]
    (when (.endsWith (str (first c)) "idx")
      (swap! state dissoc (first c)))))

(defn clear
  []
  (println "*************************************")
  (alter-var-root #'state (constantly (atom {}))))


(defn clear-lifecycle
  []
  (alter-var-root #'before-fs (constantly (atom [])))
  (alter-var-root #'after-fs (constantly (atom []))))


(defn start! [f]
  (f)
  (println @before-fs)
  (println @after-fs)
  (reduce (fn [r f]
            (f state))
          state
          @before-fs)
  (println "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"))

(defn get-state
  ([k] (get @state k))
  ([k d] (get @state k d)))

(defn after! [f]
  (swap! after-fs conj f))

(defn stop! []
  (when-not (empty? @after-fs)
    (reduce (fn [r f]
              (f state))
            state
            @after-fs)
    (clear)))


(defn only-refresh!
  []
  (refresh))

(defn refresh!
  [f]
  (stop!)
  (refresh)
  (start! #(str "starting.....")))
