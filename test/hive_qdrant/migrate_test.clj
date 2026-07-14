(ns hive-qdrant.migrate-test
  "Trifecta for migrate: golden + property + mutation."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-mcp.protocols.memory :as mem-proto]
            [hive-qdrant.migrate :as mig]))

(defrecord CountingStore [entries-atom]
  mem-proto/IMemoryStore
  (connect!       [_ _] {:success? true})
  (disconnect!    [_]   nil)
  (connected?     [_]   true)
  (health-check   [_]   {:healthy? true})
  (add-entry!     [_ e] (swap! entries-atom assoc (:id e) e) {:success? true :id (:id e)})
  (get-entry      [_ id] (get @entries-atom id))
  (update-entry!  [_ _ _] {:success? true})
  (delete-entry!  [_ _] {:success? true})
  (query-entries  [_ _] (vec (vals @entries-atom)))
  (search-similar [_ _ _] {:success? true :results []})
  (supports-semantic-search? [_] false)
  (cleanup-expired! [_] {:success? true})
  (entries-expiring-soon [_ _ _] [])
  (find-duplicate [_ _ _ _] nil)
  (store-status   [_] {:backend "stub"})
  (reset-store!   [_] nil))

(defn- stub-target [] (->CountingStore (atom {})))

(defn- fake-source [n]
  (fn [] (mapv (fn [i] {:id (str i) :type :note :content (str "c" i)}) (range n))))

;; ---- Golden ---------------------------------------------------------------

(deftest golden-sync-happy-path
  (let [tgt (stub-target)
        r   (mig/sync! {:source-fn (fake-source 5)
                        :target    tgt
                        :batch-size 2})]
    (is (= 5 (:extracted r)))
    (is (= 5 (:transformed r)))
    (is (= 5 (:loaded-ok r)))
    (is (zero? (:loaded-fail r)))
    (is (= 3 (:batches r)))))

(deftest golden-dry-run-skips-writes
  (let [tgt (stub-target)
        r   (mig/sync! {:source-fn (fake-source 3)
                        :target    tgt
                        :dry-run?  true})]
    (is (:dry-run? r))
    (is (zero? (:loaded-ok r)))
    (is (empty? @(:entries-atom tgt)))))

;; ---- Property -------------------------------------------------------------

(deftest property-transform-ensures-id-and-type
  (let [tgt (stub-target)]
    (mig/sync! {:source-fn (fn [] [{:content "no id"} {:content "also"}])
                :target    tgt})
    (doseq [e (vals @(:entries-atom tgt))]
      (is (some? (:id e)))
      (is (some? (:type e))))))

(deftest property-verify-returns-ok-count
  (let [tgt (stub-target)]
    (mig/sync! {:source-fn (fake-source 4) :target tgt})
    (let [v (mig/verify {:target tgt :ids ["0" "1" "nope"]})]
      (is (= 3 (:checked v)))
      (is (= 2 (:ok v)))
      (is (= ["nope"] (:missing v))))))

;; ---- Mutation -------------------------------------------------------------

(deftest mutation-batch-size-default
  (let [tgt (stub-target)
        r   (mig/sync! {:source-fn (fake-source 1) :target tgt})]
    (is (= 1 (:batches r)))))

(deftest mutation-missing-source-fn-throws
  (is (thrown? AssertionError
               (mig/sync! {:target (stub-target)}))))

(deftest mutation-missing-target-throws
  (is (thrown? AssertionError
               (mig/sync! {:source-fn (fake-source 1)}))))
