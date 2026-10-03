(ns hive-qdrant.ensure-collection-test
  "ensure-collection! probes collectionExistsAsync before creating, so a
   reconnect against an existing collection sends no create (whose
   ALREADY_EXISTS the java client logs at ERROR). A failed probe falls back
   to create-and-swallow.

   ensure-collection! is private and connect! opens a real client from
   config, so these tests call the var #'hive-qdrant.store/ensure-collection!
   with the fake client as the port."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.store])
  (:import [java.util.concurrent CompletableFuture]))

(def ^:private ensure-collection! #'hive-qdrant.store/ensure-collection!)

(def ^:private cfg {:collection-name "probe_test" :vector-size 4})

(deftest fresh-fake-creates-the-collection-once
  (let [c (fake/client)]
    (ensure-collection! {:client c} cfg)
    (is (= 1 (fake/create-calls c)))
    (is (= #{"probe_test"} (fake/collections c)))))

(deftest second-ensure-skips-the-create
  (testing "two ensure calls against the same fake create the collection exactly once"
    (let [c (fake/client)]
      (ensure-collection! {:client c} cfg)
      (ensure-collection! {:client c} cfg)
      (is (= 1 (fake/create-calls c))))))

(deftest failed-probe-falls-back-to-create
  (testing "a client whose exists-probe throws still reaches createCollectionAsync"
    (let [c      (fake/client)
          probed (atom 0)
          client (reify hive_qdrant.fake_qdrant.IFakeQdrant
                   (upsertAsync [_ coll pts] (.upsertAsync c coll pts))
                   (retrieveAsync [_ coll ids p v k] (.retrieveAsync c coll ids p v k))
                   (searchAsync [_ r] (.searchAsync c r))
                   (deleteAsync [_ coll x] (.deleteAsync c coll x))
                   (collectionExistsAsync [_ _]
                     (swap! probed inc)
                     (throw (RuntimeException. "UNAVAILABLE: probe down")))
                   (createCollectionAsync [_ r] (.createCollectionAsync c r))
                   (deleteCollectionAsync [_ coll] (.deleteCollectionAsync c coll)))]
      (ensure-collection! {:client client} cfg)
      (is (= 1 @probed))
      (is (= 1 (fake/create-calls c))))))

(deftest failed-probe-create-error-is-swallowed
  (testing "probe and create both failing does not throw out of ensure-collection!"
    (is (nil? (ensure-collection! {:client (fake/broken-client "boom")} cfg)))))

(deftest unanswered-probe-falls-back-to-create
  (testing "a probe future that never completes is a probe failure, not a hang"
    (let [c      (fake/client)
          client (reify hive_qdrant.fake_qdrant.IFakeQdrant
                   (upsertAsync [_ coll pts] (.upsertAsync c coll pts))
                   (retrieveAsync [_ coll ids p v k] (.retrieveAsync c coll ids p v k))
                   (searchAsync [_ r] (.searchAsync c r))
                   (deleteAsync [_ coll x] (.deleteAsync c coll x))
                   (collectionExistsAsync [_ _] (CompletableFuture.))
                   (createCollectionAsync [_ r] (.createCollectionAsync c r))
                   (deleteCollectionAsync [_ coll] (.deleteCollectionAsync c coll)))]
      (ensure-collection! {:client client} cfg)
      (is (= 1 (fake/create-calls c))))))
