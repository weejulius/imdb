(ns ^{:doc "generate uniqure id for each schema"}
  imdb.id-creator
  (:use [clojure.test]))

(def cur-id (atom 0))
(def cur-mils (atom nil))

(defn gen-id
  []
  (let [n (System/currentTimeMillis)
        m (* n 10000)
        cur-n @cur-mils]
    (if (= n cur-n) (+ m (swap! cur-id inc))
        (do (reset! cur-mils n)
            m))))


(deftest test-gen-id
  (testing ""
    (is (> 0 (gen-id)))))
