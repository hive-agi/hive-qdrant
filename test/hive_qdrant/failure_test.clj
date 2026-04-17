(ns hive-qdrant.failure-test
  "Trifecta for failure classifier: golden shapes + property + mutation."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-qdrant.failure :as f]))

;; ---- Golden shapes --------------------------------------------------------

(deftest golden-classify-transient
  (testing "UNAVAILABLE marker → :error/transient"
    (let [r (f/classify (Exception. "io.grpc.StatusRuntimeException: UNAVAILABLE: endpoint"))]
      (is (= :error/transient (:variant r)))
      (is (string? (:message r)))))
  (testing "DEADLINE_EXCEEDED marker → :error/transient"
    (is (= :error/transient
           (:variant (f/classify (Exception. "DEADLINE_EXCEEDED on stream"))))))
  (testing "connection marker → :error/transient"
    (is (= :error/transient
           (:variant (f/classify (Exception. "connection reset by peer")))))))

(deftest golden-classify-fatal
  (testing "non-marker message → :error/fatal"
    (is (= :error/fatal
           (:variant (f/classify (Exception. "collection not found")))))
    (is (= :error/fatal
           (:variant (f/classify (Exception. "bad request: wrong dims")))))))

(deftest golden-legacy-shape
  (testing "transient → reconnecting? true"
    (let [legacy (f/->legacy-map (f/classify (Exception. "UNAVAILABLE")))]
      (is (false? (:success? legacy)))
      (is (vector? (:errors legacy)))
      (is (true? (:reconnecting? legacy)))))
  (testing "fatal → reconnecting? false"
    (let [legacy (f/->legacy-map (f/classify (Exception. "schema mismatch")))]
      (is (false? (:reconnecting? legacy)))))
  (testing "reconnect-timeout is reconnecting"
    (is (true? (:reconnecting? (f/->legacy-map (f/reconnect-timeout "slow")))))))

;; ---- Property tests -------------------------------------------------------

(deftest property-classify-total
  (testing "classify never throws, always returns :variant + :message"
    (doseq [msg ["" "random noise" "UNAVAILABLE" nil "null"
                 "DEADLINE_EXCEEDED: 500ms" "foo bar baz" "connection"]]
      (let [r (f/classify (Exception. (str msg)))]
        (is (contains? r :variant))
        (is (contains? r :message))
        (is (#{:error/transient :error/fatal :error/reconnect-timeout} (:variant r)))))))

(deftest property-transient-predicate-agrees
  (testing "transient? agrees with :variant"
    (let [t (f/classify (Exception. "UNAVAILABLE"))
          g (f/classify (Exception. "schema mismatch"))]
      (is (f/transient? t))
      (is (not (f/transient? g))))))

;; ---- Mutation — kills common bugs -----------------------------------------

(deftest mutation-nil-message
  (testing "null message does not NPE"
    (is (map? (f/classify (Exception.))))))

(deftest mutation-case-insensitive
  (testing "marker match is case-insensitive"
    (is (= :error/transient (:variant (f/classify (Exception. "unavailable")))))
    (is (= :error/transient (:variant (f/classify (Exception. "UnAvAiLaBlE")))))))
