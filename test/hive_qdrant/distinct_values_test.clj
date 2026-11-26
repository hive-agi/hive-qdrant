(ns hive-qdrant.distinct-values-test
  "The :distinct-values capability: every value of one payload field with
   its count, exact or refused, over the live branch (the fake client's
   facet, which truncates silently as qdrant does) and the fallback."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.store :as store]
            [hive-spi.memory.ports :as proto]))

(defn- live-store
  ([] (live-store (fake/client {})))
  ([client]
   (circuit/force-reset!)
   (let [s (store/create-store {:collection-name "test-distinct" :vector-size 4})]
     (reset! (:client-atom s) {:client client})
     (reset! (:connected?-atom s) true)
     s)))

(defn- fallback-store []
  (circuit/force-reset!)
  (store/create-store {:vector-size 4}))

(defn- seed! [s pids]
  (doseq [[i pid] (map-indexed vector pids)]
    (proto/add-entry! s {:id (format "%014d-00000000" i) :type "note" :tags ["carto"]
                         :project-id pid :content (str "row " i)})))

(def ^:private pids ["a" "b" "a" "c" "a" "b"])

(def ^:private expected [{:value "a" :count 3} {:value "b" :count 2} {:value "c" :count 1}])

(deftest the-capability-is-declared
  (is (some #{:distinct-values} (:capabilities (proto/store-status (fallback-store))))))

(deftest live-distinct-values-are-exact
  (let [s (live-store)]
    (seed! s pids)
    (is (= expected (store/distinct-values s "project-id" {})))
    (is (= expected (store/distinct-values s :project-id {:limit 3})) "exactly at the limit is whole")
    (testing "tags: a list value counts each element"
      (is (= [{:value "carto" :count 6}] (store/distinct-values s "tags" {}))))))

(deftest a-cut-facet-throws-instead-of-answering-a-prefix
  (let [s (live-store)]
    (seed! s pids)
    (let [e (try (store/distinct-values s "project-id" {:limit 2}) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :qdrant/distinct-values-truncated (:error (ex-data e))))
      (is (= 2 (:limit (ex-data e)))))))

(deftest a-failed-facet-throws-and-trips-the-circuit
  (let [s (live-store (fake/broken-client "FATAL: boom"))]
    (is (thrown? clojure.lang.ExceptionInfo (store/distinct-values s "project-id" {}))
        "a failed read is not []")
    (is (= :distinct-values (:op (try (store/distinct-values s "project-id" {}) nil
                                      (catch clojure.lang.ExceptionInfo e (ex-data e))))))))

(deftest fallback-distinct-values-honour-the-filter
  (let [s (fallback-store)]
    (seed! s pids)
    (is (= expected (store/distinct-values s "project-id" {})))
    (is (= [{:value "b" :count 2}] (store/distinct-values s "project-id" {:project-id "b"})))
    (is (thrown? clojure.lang.ExceptionInfo (store/distinct-values s "project-id" {:limit 1})))))
