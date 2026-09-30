(ns hive-qdrant.store-conformance-test
  "QdrantMemoryStore held to hive-spi's IMemoryStore conformance suite, on its
   in-memory fallback branch and on its live branch over the fake client."
  (:require [clojure.test :refer [deftest is]]
            [hive-spi.memory.conformance :as conformance]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.store :as store]))

(def ^:private vsize 4)

(defn- fallback-store []
  (circuit/force-reset!)
  (store/create-store {:vector-size vsize}))

(defn- live-store []
  (circuit/force-reset!)
  (let [s (store/create-store {:collection-name "test-conformance" :vector-size vsize})]
    (reset! (:client-atom s) {:client (fake/client {})})
    (reset! (:connected?-atom s) true)
    s))

(def ^:private write-cases
  [:add-returns-id :add-generates-id :update-merges :write-returns-honour-contract
   :update-unknown-is-absent :delete-then-get-is-nil :delete-unknown-is-true
   :delete-unknown-does-not-throw :metadata-write])

(deftest fallback-honours-the-write-contract
  (doseq [id write-cases]
    (is (= :ran (conformance/run-case fallback-store {} id)) (str id))))

(deftest live-honours-the-write-contract
  (doseq [id write-cases]
    (is (= :ran (conformance/run-case live-store {} id)) (str id))))
