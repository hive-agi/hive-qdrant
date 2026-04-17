(ns hive-qdrant.queue-test
  "Trifecta for write queue: golden + property + mutation."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-qdrant.queue :as q]))

(use-fixtures :each (fn [f] (q/clear!) (f) (q/clear!)))

;; ---- Golden ---------------------------------------------------------------

(deftest golden-enqueue-accept
  (let [r (q/enqueue! {:op :add-entry! :id "a" :args [{:id "a"}]})]
    (is (:success? r))
    (is (:queued? r))
    (is (= 1 (:depth r)))
    (is (vector? (:tips r)))))

(deftest golden-degraded-response-shape
  (let [r (q/degraded-response {:operation :get-entry})]
    (is (false? (:success? r)))
    (is (true?  (:degraded? r)))
    (is (= "qdrant" (:backend r)))
    (is (true?  (:reconnecting? r)))
    (is (vector? (:tips r)))))

(deftest golden-coalesce-collapses-same-id
  (let [ops [{:op :add-entry! :id "a" :args [{:v 1}]}
             {:op :add-entry! :id "b" :args [{:v 1}]}
             {:op :add-entry! :id "a" :args [{:v 2}]}]
        out (q/coalesce ops)]
    (is (= 2 (count out)))
    (is (= 2 (-> (first (filter #(= "a" (:id %)) out)) :args first :v)))))

;; ---- Property -------------------------------------------------------------

(deftest property-fifo-order-preserved
  (let [ids (mapv str (range 10))
        ops (mapv (fn [i] {:op :add-entry! :id i :args [{:id i}]}) ids)]
    (doseq [op ops] (q/enqueue! op))
    (is (= 10 (q/size)))
    (let [coalesced (q/coalesce ops)]
      (is (= ids (mapv :id coalesced))))))

(deftest property-drain-all-success
  (doseq [i (range 5)]
    (q/enqueue! {:op :add-entry! :id (str i) :args [{:id (str i)}]}))
  (let [log (atom [])
        r   (q/drain! {:dispatch-fn (fn [op] (swap! log conj (:id op)) :ok)})]
    (is (= :drained (:result r)))
    (is (= 5 (:succeeded r)))
    (is (zero? (q/size)))
    (is (= 5 (count @log)))))

(deftest property-drain-zero-progress-backs-off
  (q/enqueue! {:op :add-entry! :id "x" :args [{}]})
  (let [r (q/drain! {:dispatch-fn (fn [_] (throw (Exception. "boom")))})]
    (is (= :all-failed (:result r)))
    (is (pos? (:failed r)))
    ;; failed op re-queued
    (is (pos? (q/size)))))

;; ---- Mutation -------------------------------------------------------------

(deftest mutation-stats-map-shape
  (let [s (q/stats)]
    (is (contains? s :enqueued))
    (is (contains? s :depth))
    (is (contains? s :max-size))
    (is (contains? s :draining?))))

(deftest mutation-coalesce-keeps-latest
  (let [ops [{:op :update-entry! :id "k" :args ["k" {:v 1}]}
             {:op :update-entry! :id "k" :args ["k" {:v 99}]}]
        out (q/coalesce ops)]
    (is (= 1 (count out)))
    (is (= 99 (-> out first :args second :v)))))

(deftest mutation-clear-resets-depth
  (q/enqueue! {:op :add-entry! :id "z" :args [{}]})
  (is (pos? (q/size)))
  (q/clear!)
  (is (zero? (q/size))))
