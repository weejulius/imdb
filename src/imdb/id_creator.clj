(ns ^{:doc "generate uniqure id for each schema"}
  imdb.id-creator
  (:use [clojure.test]))

(def cur-id (atom 0))
(def cur-mils (atom nil))

(defn gen-id-by-time
  []
  (let [n (System/currentTimeMillis)
        m (* n 10000)
        cur-n @cur-mils]
    (if (= n cur-n) (+ m (swap! cur-id inc))
        (do (reset! cur-mils n)
            m))))

(def seq-ids (atom {}))

(defn gen-id
  "gen sequential id for each subject"
  [sub]
  (let [^java.util.concurrent.atomic.AtomicLong cur-id (sub @seq-ids)]
    (if-not cur-id
      (do (swap! seq-ids
                 (fn [c]
                   (assoc c sub (java.util.concurrent.atomic.AtomicLong. 1))))
          1)
      (.incrementAndGet cur-id))))

(deftest test-gen-id
  (testing ""
    (is (= 1 (gen-id :h)))
    (is (= 2 (gen-id :h)))
    (is (= 3 (gen-id :h)))
    (is (= 1 (gen-id :i)))))

(deftest test-gen-id-by-time
  (testing ""
    (is (> 0 (gen-id)))))
