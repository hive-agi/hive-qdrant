(ns hive-qdrant.batch-delete-test
  "IMemoryStoreBatchDelete on QdrantMemoryStore: one points-delete for the
   whole id list on the live branch (over the fake client, which counts the
   delete calls), the in-memory fallback, and hive-spi's :batch-delete
   conformance case on both branches. The port and the case are resolved by
   name: against a hive-spi without them these tests pass vacuously."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.store :as store]
            [hive-spi.memory.conformance :as conformance]
            [hive-spi.memory.ports :as proto])
  (:import [hive_qdrant.fake_qdrant IFakeQdrant]))

(def ^:private port (some-> (ns-resolve 'hive-spi.memory.ports 'IMemoryStoreBatchDelete) deref))

(defn- delete-entries! [s ids] ((ns-resolve 'hive-spi.memory.ports 'delete-entries!) s ids))

(def ^:private case? (some #{:batch-delete} conformance/case-ids))

(defn- fallback-store []
  (circuit/force-reset!)
  (store/create-store {:vector-size 4}))

(defn- live-store
  ([] (live-store (fake/client {})))
  ([client]
   (circuit/force-reset!)
   (let [s (store/create-store {:collection-name "test-batch-delete" :vector-size 4})]
     (reset! (:client-atom s) {:client client})
     (reset! (:connected?-atom s) true)
     s)))

(defn- counting-client
  "The fake client, with every deleteAsync call counted into CALLS."
  [calls]
  (let [inner (fake/client {})]
    (reify IFakeQdrant
      (upsertAsync [_ c ps] (.upsertAsync inner c ps))
      (retrieveAsync [_ c ids p v k] (.retrieveAsync inner c ids p v k))
      (searchAsync [_ r] (.searchAsync inner r))
      (scrollAsync [_ r] (.scrollAsync inner r))
      (countAsync [_ c f e] (.countAsync inner c f e))
      (deleteAsync [_ c ids]
        (swap! calls inc)
        (.deleteAsync inner c ids))
      (collectionExistsAsync [_ c] (.collectionExistsAsync inner c))
      (createCollectionAsync [_ r] (.createCollectionAsync inner r))
      (deleteCollectionAsync [_ c] (.deleteCollectionAsync inner c)))))

(defn- seed! [s n]
  (mapv (fn [i]
          (proto/add-entry! s {:id (format "%014d-0000000%d" i (mod i 10)) :type "note"
                               :tags ["carto"] :project-id "p" :content (str "row " i)}))
        (range n)))

(deftest store-implements-the-port
  (when port
    (is (satisfies? port (fallback-store)))
    (is (satisfies? port (live-store)))))

(deftest live-batch-is-one-round-trip
  (when port
    (let [calls (atom 0)
          s     (live-store (counting-client calls))
          ids   (seed! s 6)
          r     (delete-entries! s (concat (take 4 ids) [(first ids) "missing-id"]))]
      (is (= 1 @calls) "four ids, a repeat and an unknown id: one points-delete")
      (is (= 5 (count r)) "one result per distinct id")
      (is (every? true? r))
      (is (every? nil? (map #(proto/get-entry s %) (take 4 ids))))
      (is (every? some? (map #(proto/get-entry s %) (drop 4 ids))) "ids not listed survive")
      (testing "no ids, no round trip"
        (is (= [] (delete-entries! s [])))
        (is (= 1 @calls))))))

(deftest live-failure-falls-back-to-single-deletes
  (when port
    (let [s (live-store (fake/broken-client "UNAVAILABLE: io exception"))
          r (delete-entries! s ["a" "b"])]
      (is (= 2 (count r)) "every id still answers, as delete-entry! would"))))

(deftest fallback-batch-delete
  (when port
    (let [s   (fallback-store)
          ids (seed! s 3)]
      (is (= [true true] (delete-entries! s (take 2 ids))))
      (is (nil? (proto/get-entry s (first ids))))
      (is (some? (proto/get-entry s (last ids)))))))

(deftest conformance-batch-delete
  (when (and port case?)
    (is (= :ran (conformance/run-case fallback-store {} :batch-delete)))
    (is (= :ran (conformance/run-case live-store {} :batch-delete)))))
