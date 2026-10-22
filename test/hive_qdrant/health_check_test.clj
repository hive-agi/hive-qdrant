(ns hive-qdrant.health-check-test
  "health-check gates :healthy? on the configured collection existing, not
   on reachability alone. Driven through the fake client: no live qdrant."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.store :as store]
            [hive-spi.memory.ports :as proto]))

(use-fixtures :each
  (fn [f] (circuit/force-reset!) (f) (circuit/force-reset!)))

(def ^:private coll "health_test")

(defn- store-over
  "A store wired to CLIENT as if connect! had succeeded."
  [client]
  (let [s (store/create-store {:collection-name coll :vector-size 4})]
    (reset! (:client-atom s) {:client client})
    (reset! (:connected?-atom s) true)
    s))

(deftest connected-without-collection-is-unhealthy
  (let [h (proto/health-check (store-over (fake/client)))]
    (is (false? (:healthy? h)))
    (is (= :down (:status h)))
    (is (= coll (:collection h)))
    (is (= ["Collection does not exist"] (:errors h)))))

(deftest connected-with-collection-is-healthy
  (let [c (fake/client)]
    (#'store/ensure-collection! {:client c} {:collection-name coll :vector-size 4})
    (let [h (proto/health-check (store-over c))]
      (is (true? (:healthy? h)))
      (is (= :ok (:status h)))
      (is (= coll (:collection h)))
      (is (= [] (:errors h))))))

(deftest another-collection-does-not-count
  (let [c (fake/client)]
    (#'store/ensure-collection! {:client c} {:collection-name "other" :vector-size 4})
    (is (false? (:healthy? (proto/health-check (store-over c)))))))

(deftest disconnected-is-unhealthy
  (testing "never connected"
    (let [h (proto/health-check (store/create-store {:collection-name coll}))]
      (is (false? (:healthy? h)))
      (is (= :down (:status h)))
      (is (= ["Not connected"] (:errors h)))))
  (testing "disconnected after holding a collection"
    (let [c (fake/client)]
      (#'store/ensure-collection! {:client c} {:collection-name coll :vector-size 4})
      (let [s (store-over c)]
        (proto/disconnect! s)
        (is (false? (:healthy? (proto/health-check s))))))))

(deftest failed-probe-is-unhealthy
  (let [h (proto/health-check (store-over (fake/broken-client "UNAVAILABLE: down")))]
    (is (false? (:healthy? h)))
    (is (= ["exists probe failed"] (:errors h)))))
