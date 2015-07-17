(ns imdb.common
  (:use [clojure.test]))


(defn in?
  "true if seq contains elm"
  [seq elm]
  (some #(= elm %) seq))


(defmacro defcurrable
  "define the currable function, p is the params to be curried"
  [name doc p p1 body]
  `(defn ~name ~doc ~p1 (fn ~p ~body)))

(defmacro cur
  [name f & p]
  `(def ~name (~f ~@p)))




(defn <<
  [& items]
  (reduce
   (fn
     [r k]
     (if r (r k) r))
   (first items)
   (rest items)))

(deftest test-<<
  (testing ""
    (is (= 2 (<< inc 1 )))))
