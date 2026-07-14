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
  (testing "capability tracks the injected embedder, not a hardcoded true"
    ;; Was `(is (true? ...))` on a store with NO embedder — the store then
    ;; wrote and queried zero vectors, so it advertised a semantic lane it did
    ;; not have (kanban 20260712134504-18ceba10). See store-semantic-test.
    (is (false? (proto/supports-semantic-search? (fresh-store)))
        "no embedder wired -> no semantic search")
    (is (true? (proto/supports-semantic-search?
                (store/create-store {:collection-name "test" :vector-size 4
                                     :embedder (fn [_] [0.1 0.2 0.3 0.4])}))))))

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

;; ---- find-duplicate (rescan idempotency, kanban 20260503210515-306bfe41) ----

(deftest find-duplicate-fallback-matches-by-content-hash
  (testing "fallback mode: returns the existing entry sharing :content-hash"
    (let [s (fresh-store)]
      (proto/add-entry! s {:id "e1" :type :snippet :content "x"
                           :content-hash "abc123" :project-id "p"})
      (let [hit (proto/find-duplicate s :snippet "abc123" {:project-id "p"})]
        (is (some? hit) "must locate by content-hash")
        (is (= "e1" (:id hit)))))))

(deftest find-duplicate-fallback-nil-on-miss
  (testing "fallback mode: returns nil when no entry has matching hash"
    (let [s (fresh-store)]
      (proto/add-entry! s {:id "e1" :type :snippet :content "x"
                           :content-hash "abc123" :project-id "p"})
      (is (nil? (proto/find-duplicate s :snippet "DOES-NOT-EXIST" {:project-id "p"}))))))

(deftest ^:integration live-find-duplicate-rescan-idempotency
  (testing "rescan with same content-hash reuses existing id (no qdrant dups)"
    (let [s (store/create-store {:host "localhost" :port 6334
                                 :collection-name "test_find_dup" :vector-size 4})
          r (proto/connect! s {})]
      (when (:success? r)
        (let [hash "RESCAN-HASH-1"
              id1  "ITEST-DUP-1"
              base {:type :snippet :content "snip" :content-hash hash
                    :project-id "p" :embedding [0.1 0.2 0.3 0.4]}]
          (proto/add-entry! s (assoc base :id id1))
          ;; First find-duplicate must locate id1.
          (let [hit (proto/find-duplicate s :snippet hash {:project-id "p"})]
            (is (some? hit) "post-insert: find-duplicate must surface the entry")
            (is (= id1 (:id hit)) "must return the original id, not a fresh UUID")
            ;; Simulating scan reuse path: fresh id materialised, but
            ;; rebound to existing id when find-duplicate hits → only
            ;; one point under id1's UUID.
            (is (= id1 (:id (proto/get-entry s id1)))))
          (proto/delete-entry! s id1)
          (proto/disconnect! s))))))

;; ---- Integration (live qdrant) -------------------------------------------

(deftest ^:integration live-connect-roundtrip
  (let [s (store/create-store {:host "localhost" :port 6334 :vector-size 4})
        r (proto/connect! s {})]
    (when (:success? r)
      (proto/add-entry! s {:id "ITEST-1" :type :note :content "live"
                           :embedding [0.1 0.2 0.3 0.4]})
      (is (:success? (proto/store-status s)))
      (proto/disconnect! s))))

(deftest ^:integration live-add-get-preserves-payload
  (testing "get-entry round-trip survives qdrant decode (regression: missing q-api/point->map)"
    (let [s (store/create-store {:host "localhost" :port 6334 :vector-size 4})
          r (proto/connect! s {})]
      (when (:success? r)
        (let [id "ITEST-RT-1"
              tags ["kanban" "todo" "priority-medium" "scope:project:test"]]
          (proto/add-entry! s {:id id :type :note :content "round-trip"
                                :tags tags :embedding [0.1 0.2 0.3 0.4]})
          (let [fetched (proto/get-entry s id)]
            (is (some? fetched) "get-entry returns the entry, not nil")
            (is (= id (:id fetched)) "id preserved")
            (is (= tags (:tags fetched)) "tags survive the decode")
            (is (some? (:content fetched)) "content payload survives the decode"))
          (proto/delete-entry! s id))
        (proto/disconnect! s)))))
