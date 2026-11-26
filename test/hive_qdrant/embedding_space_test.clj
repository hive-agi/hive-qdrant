(ns hive-qdrant.embedding-space-test
  "ensure-collection! guards a collection's embedding space, not only its
   dimension: a new collection is stamped with the space in its qdrant
   metadata, an existing one must hold the same space (another space throws
   :embedding-space-mismatch), and an unstamped legacy collection is admitted
   with a warning. Driven over the fake client, no cluster."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-qdrant.connect :as connect]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.space :as espace]
            [hive-spi.embeddings.space :as space]))

(use-fixtures :each (fn [t] (espace/reset-legacy-warnings!) (t)))

(def ^:private qwen
  {:provider :ollama :model "qwen3-embedding:4b" :digest "sha256:aaa"
   :dimension 4 :query-template-rev "raw" :doc-serializer-rev "memory-to-document/1"})

(defn- cfg [space]
  (cond-> {:collection-name "space_test" :vector-size 4 :distance :cosine}
    space (assoc :embedding-space space)))

(defn- stamp-of [c] (espace/qdrant-metadata->stamp (fake/metadata c "space_test")))

(deftest metadata-round-trips-the-space
  (let [s (space/->space (assoc qwen :metric :cosine))]
    (is (= s (space/metadata->space
              (espace/qdrant-metadata->stamp (espace/space->qdrant-metadata s)))))))

(deftest distance-sets-the-metric
  (is (= :cosine (:metric (espace/space-for-distance qwen :cosine))))
  (is (= :l2 (:metric (espace/space-for-distance qwen :euclid))))
  (is (= :ip (:metric (espace/space-for-distance qwen :dot))))
  (testing "a distance with no space metric, or no space, guards nothing"
    (is (nil? (espace/space-for-distance qwen :manhattan)))
    (is (nil? (espace/space-for-distance nil :cosine)))))

(deftest new-collection-is-stamped
  (let [c (fake/client)]
    (connect/ensure-collection! {:client c} (cfg qwen))
    (is (= 1 (fake/create-calls c)))
    (is (= (space/space-id (espace/space-for-distance qwen :cosine))
           (:embed-space-id (stamp-of c))))
    (is (= "sha256:aaa" (:embed-digest (stamp-of c))))))

(deftest no-space-creates-unstamped
  (let [c (fake/client)]
    (connect/ensure-collection! {:client c} (cfg nil))
    (is (= 1 (fake/create-calls c)))
    (is (= {} (stamp-of c)))))

(deftest reopen-with-the-same-space-is-admitted
  (let [c (fake/client)]
    (connect/ensure-collection! {:client c} (cfg qwen))
    (is (nil? (connect/ensure-collection! {:client c} (cfg qwen))))
    (is (= 1 (fake/create-calls c)) "the reopen sends no create")))

(deftest reopen-with-another-digest-is-refused
  (let [c (fake/client)]
    (connect/ensure-collection! {:client c} (cfg qwen))
    (let [e (try (connect/ensure-collection! {:client c} (cfg (assoc qwen :digest "sha256:bbb")))
                 nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e) "a same-dimension, other-digest space is not admitted")
      (is (= :embedding-space-mismatch (:error (ex-data e))))
      (is (= "space_test" (:collection (ex-data e))))
      (is (= [:digest] (:differs-on (ex-data e))))
      (is (= #{:isolate :re-embed} (:remedy (ex-data e)))))))

(deftest reopen-with-another-serializer-is-refused
  (let [c (fake/client)]
    (connect/ensure-collection! {:client c} (cfg qwen))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Embedding space mismatch"
                          (connect/ensure-collection!
                           {:client c} (cfg (assoc qwen :doc-serializer-rev "memory-to-document/2")))))))

(deftest legacy-collection-is-admitted
  (testing "a collection created before stamping (no metadata) is admitted, not refused"
    (let [c (fake/seed-collection! (fake/client) "space_test" {})]
      (is (nil? (connect/ensure-collection! {:client c} (cfg qwen))))
      (is (zero? (fake/create-calls c)))
      (is (= {} (stamp-of c)) "a legacy collection is reported, never silently stamped"))))

(deftest lost-create-race-still-checks-the-space
  (testing "probe says absent, create answers ALREADY_EXISTS: the winner's stamp is checked"
    (let [c      (fake/seed-collection! (fake/client) "space_test"
                                        (espace/space->qdrant-metadata
                                         (espace/space-for-distance (assoc qwen :digest "sha256:bbb") :cosine)))
          client (reify hive_qdrant.fake_qdrant.IFakeQdrant
                   (collectionExistsAsync [_ _]
                     (java.util.concurrent.CompletableFuture/completedFuture false))
                   (getCollectionInfoAsync [_ coll] (.getCollectionInfoAsync c coll))
                   (createCollectionAsync [_ r] (.createCollectionAsync c r)))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (connect/ensure-collection! {:client client} (cfg qwen)))))))

(deftest unreadable-metadata-admits-unverified
  (let [c      (fake/seed-collection! (fake/client) "space_test" {})
        client (reify hive_qdrant.fake_qdrant.IFakeQdrant
                 (collectionExistsAsync [_ coll] (.collectionExistsAsync c coll))
                 (getCollectionInfoAsync [_ _] (throw (RuntimeException. "INTERNAL: boom"))))]
    (is (nil? (connect/ensure-collection! {:client client} (cfg qwen))))))
