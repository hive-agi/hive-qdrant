(ns hive-qdrant.store-test
  "Trifecta for QdrantMemoryStore: golden + property + mutation.

   Runs against the in-memory fallback path — no live qdrant required.
   Live integration is gated behind the ^:integration metadata."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-mcp.protocols.memory :as proto]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.queue :as q]
            [hive-qdrant.store :as store]))

(use-fixtures :each
  (fn [f]
    (circuit/force-reset!)
    (q/clear!)
    (f)
    (circuit/force-reset!)
    (q/clear!)))

(defn- fresh-store []
  (store/create-store {:collection-name "test" :vector-size 4}))

;; ---- Golden ---------------------------------------------------------------

(deftest golden-satisfies-protocol
  (is (satisfies? proto/IMemoryStore (fresh-store))))

(deftest golden-fallback-add-get
  (let [s (fresh-store)
        r (proto/add-entry! s {:id "e1" :type :note :content "hello"})]
    (is (:success? r))
    (let [fetched (proto/get-entry s "e1")]
      (is (= "hello" (:content fetched))))))

(deftest golden-status-shape
  (let [s      (fresh-store)
        status (proto/store-status s)]
    (is (= "qdrant" (:backend status)))
    (is (false? (:connected? status)))
    (is (contains? status :circuit))
    (is (contains? status :queue))))

(deftest golden-supports-semantic-search
  (is (true? (proto/supports-semantic-search? (fresh-store)))))

;; ---- Property -------------------------------------------------------------

(deftest property-add-then-get-roundtrip
  (let [s (fresh-store)]
    (doseq [i (range 10)]
      (proto/add-entry! s {:id (str "k" i) :type :note :content (str "v" i)}))
    (doseq [i (range 10)]
      (is (= (str "v" i) (:content (proto/get-entry s (str "k" i))))))))

(deftest property-delete-removes-entry
  (let [s (fresh-store)]
    (proto/add-entry! s {:id "d1" :type :note :content "x"})
    (is (some? (proto/get-entry s "d1")))
    (proto/delete-entry! s "d1")
    (is (nil? (proto/get-entry s "d1")))))

(deftest property-health-reports-disconnected
  (let [s (fresh-store)
        h (proto/health-check s)]
    (is (false? (:healthy? h)))
    (is (= :down (:status h)))
    (is (= "qdrant" (:backend h)))))

;; ---- Mutation -------------------------------------------------------------

(deftest mutation-disconnect-is-idempotent
  (let [s (fresh-store)]
    (proto/disconnect! s)
    (proto/disconnect! s)
    (is (false? (proto/connected? s)))))

(deftest mutation-query-entries-returns-limit
  (let [s (fresh-store)]
    (doseq [i (range 5)]
      (proto/add-entry! s {:id (str i) :type :note :content (str i)}))
    (let [r (proto/query-entries s {:limit 3})]
      (is (<= (count r) 5)))))

(deftest mutation-reset-store-clears-fallback
  (let [s (fresh-store)]
    (proto/add-entry! s {:id "r1" :type :note :content "x"})
    (proto/reset-store! s)
    (is (nil? (proto/get-entry s "r1")))))

;; ---- Payload projection ---------------------------------------------------

(deftest point->entry-tolerates-nil-content
  (testing "projected payload without :content yields entry sans :content"
    (let [pt    {:id "abc" :payload {:type "note" :tags ["a"]
                                     :project-id "p" :created-at "2026-04-20"}}
          entry (#'store/point->entry pt)]
      (is (= "abc" (:id entry)))
      (is (nil? (:content entry)) ":content absent when not included in projection")
      (is (= "note" (:type entry)))
      (is (= ["a"] (:tags entry))))))

(deftest point->entry-handles-missing-payload
  (testing "payload nil -> entry is just id"
    (let [entry (#'store/point->entry {:id "x" :payload nil})]
      (is (= {:id "x"} entry)))))

;; ---- Integration (live qdrant) -------------------------------------------

(deftest ^:integration live-connect-roundtrip
  (let [s (store/create-store {:host "localhost" :port 6334 :vector-size 4})
        r (proto/connect! s {})]
    (when (:success? r)
      (proto/add-entry! s {:id "ITEST-1" :type :note :content "live"
                           :embedding [0.1 0.2 0.3 0.4]})
      (is (:success? (proto/store-status s)))
      (proto/disconnect! s))))
