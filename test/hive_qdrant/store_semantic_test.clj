(ns hive-qdrant.store-semantic-test
  "The semantic lane of QdrantMemoryStore (kanban 20260712134504-18ceba10).

   Three defects stacked here, all of which these tests pin:

   1. `search-similar` handed back whatever the search boundary returned —
      undecoded points — so carto's normalize-semantic degraded every row to
      an :opaque map with no :qn and semantic-grep dropped them all.
   2. `search-similar` destructured only {:keys [limit embedding]}: the
      :project-ids/:type filters were silently discarded, so even a working
      vector search answered cross-project.
   3. There was no embedder on either side. The query and every written point
      got `(zero-vec 384)`, which qdrant matches against zero data vectors at
      score 0.0 — arbitrary results, reported as success, while
      `supports-semantic-search?` hardcoded `true`.

   No live qdrant: the client seam is stubbed and the q-api boundary redef'd."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clj-qdrant.api :as q-api]
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

(def ^:private vsize 4)

(defn- connected-store
  "Store with a stubbed client so the live branch (not the in-memory
   fallback) is exercised. `extra` merges into the store config."
  [extra]
  (let [s (store/create-store (merge {:collection-name "test-carto"
                                      :vector-size     vsize}
                                     extra))]
    (reset! (:client-atom s) {:client ::stub})
    (reset! (:connected?-atom s) true)
    s))

(defn- unit-embedder
  "Deterministic (fn [text] -> vector) of width `n`."
  ([] (unit-embedder vsize))
  ([n] (fn [_text] (vec (repeat n 0.25)))))

;; =============================================================================
;; Decode — search-similar must return entry maps, never opaque points
;; =============================================================================

(deftest search-similar-returns-decoded-entry-maps
  (testing "rows are maps carrying :tags/:content/:score — not opaque points"
    (let [s (connected-store {:embedder (unit-embedder)})]
      (with-redefs [q-api/search-points
                    (fn [_client & _]
                      {:count 2
                       :results [{:id "u1" :score 0.91
                                  :payload {:id "e1" :type "snippet"
                                            :tags ["carto" "qn:foo/bar"]
                                            :content "(defn bar [] 1)"}}
                                 {:id "u2" :score 0.42
                                  :payload {:id "e2" :type "snippet"
                                            :tags ["carto" "qn:foo/baz"]
                                            :content "(defn baz [] 2)"}}]})]
        (let [res  (proto/search-similar s "make a bar" {:limit 2})
              rows (:results res)]
          (is (true? (:success? res)))
          (is (= 2 (:count res)))
          (is (every? map? rows) "a raw scored point is unreadable downstream")
          (is (= ["e1" "e2"] (mapv :id rows)) "payload :id wins over the point uuid")
          (is (= ["carto" "qn:foo/bar"] (:tags (first rows))) ":tags survive the decode")
          (is (some? (:content (first rows))) ":content survives the decode")
          (is (every? number? (map :score rows)) "score is what ranks the hit")
          (is (apply distinct? (map :score rows))
              "distinct scores — all-equal 0.0 is the zero-vector signature"))))))

;; =============================================================================
;; Filters — :project-ids / :type must reach qdrant
;; =============================================================================

(deftest search-similar-passes-project-ids-as-filter
  (testing "the scope filter is built and handed to the search boundary"
    (let [s        (connected-store {:embedder (unit-embedder)})
          captured (atom nil)]
      (with-redefs [q-api/search-points
                    (fn [_client & {:as opts}]
                      (reset! captured opts)
                      {:count 0 :results []})]
        (proto/search-similar s "parse edn config"
                              {:limit 5 :project-ids ["hive-mcp"] :type :snippet})
        (is (some? (:filter @captured))
            ":project-ids/:type were silently discarded before this fix")
        (is (true? (:with-payload? @captured))
            "without with_payload qdrant returns id+score only")))))

(deftest search-similar-without-filters-sends-none
  (testing "no scope opts -> no filter (match-all), not an empty filter object"
    (let [s        (connected-store {:embedder (unit-embedder)})
          captured (atom nil)]
      (with-redefs [q-api/search-points
                    (fn [_client & {:as opts}]
                      (reset! captured opts)
                      {:count 0 :results []})]
        (proto/search-similar s "q" {:limit 5})
        (is (nil? (:filter @captured)))))))

;; =============================================================================
;; The vector lane — a zero vector is never a legitimate value
;; =============================================================================

(deftest search-similar-embeds-the-query-text
  (testing "the store embeds query-text itself (IMemoryStore contract)"
    (let [s        (connected-store {:embedder (unit-embedder)})
          captured (atom nil)]
      (with-redefs [q-api/search-points
                    (fn [_client & {:as opts}]
                      (reset! captured opts)
                      {:count 0 :results []})]
        (proto/search-similar s "parse edn config" {:limit 3})
        (let [v (:vector @captured)]
          (is (= vsize (count v)))
          (is (not (every? zero? v))
              "a zero query vector scores 0.0 against every zero data vector"))))))

(deftest search-similar-without-embedder-fails-loud
  (testing "no embedding lane -> an error envelope, NOT an empty success"
    (let [s      (connected-store {})
          called (atom false)]
      (with-redefs [q-api/search-points (fn [& _] (reset! called true)
                                          {:count 0 :results []})]
        (let [res (proto/search-similar s "anything" {:limit 3})]
          (is (false? (:success? res)))
          (is (= :qdrant/no-embedder (:error res)))
          (is (false? @called) "must not query qdrant with a fabricated vector"))))))

(deftest add-entry-never-writes-a-zero-vector
  (testing "with an embedder wired, the written point carries the real vector"
    (let [s     (connected-store {:embedder (unit-embedder)})
          point (atom nil)]
      (with-redefs [q-api/upsert-points
                    (fn [_client & {:keys [points]}]
                      (reset! point (first points))
                      {:count 1})]
        (let [id (proto/add-entry! s {:id "e1" :type :snippet :content "(defn f [])"})]
          (is (= "e1" id))
          (is (= vsize (count (:vector @point))))
          (is (not (every? zero? (:vector @point)))
              "all-zero vectors are how 116k unrankable carto points were written"))))))

(deftest add-entry-refuses-a-dimension-mismatch
  (testing "an embedder of the wrong width is a hard error, never an upsert"
    (let [s       (connected-store {:embedder (unit-embedder 8)})
          upserts (atom 0)]
      (with-redefs [q-api/upsert-points (fn [& _] (swap! upserts inc) {:count 1})]
        (let [res (proto/add-entry! s {:id "e1" :type :snippet :content "x"})]
          (is (map? res) "must not report a successful id")
          (is (= :qdrant/dim-mismatch (:error res)))
          (is (= 8 (:actual res)))
          (is (= vsize (:expected res)))
          (is (zero? @upserts) "nothing may reach the collection"))))))

(deftest add-entry-refuses-when-embedder-fails
  (testing "an embedder that throws does not degrade to a zero vector"
    (let [s       (connected-store {:embedder (fn [_] (throw (ex-info "ollama down" {})))})
          upserts (atom 0)]
      (with-redefs [q-api/upsert-points (fn [& _] (swap! upserts inc) {:count 1})]
        (let [res (proto/add-entry! s {:id "e1" :type :snippet :content "x"})]
          (is (= :qdrant/embed-failed (:error res)))
          (is (zero? @upserts)))))))

(deftest add-entry-honors-an-explicit-embedding
  (testing "a caller-supplied :embedding is used as-is (migration path)"
    (let [s     (connected-store {})
          point (atom nil)]
      (with-redefs [q-api/upsert-points
                    (fn [_client & {:keys [points]}] (reset! point (first points)) {:count 1})]
        (proto/add-entry! s {:id "e1" :type :note :content "x"
                             :embedding [0.1 0.2 0.3 0.4]})
        (is (= [0.1 0.2 0.3 0.4] (:vector @point)))))))

(deftest structural-slot-still-writes
  (testing "a slot with NO embedder (kanban) keeps writing — placeholder vector,
            but it reports honestly that it cannot do semantic search"
    (let [s     (connected-store {})
          point (atom nil)]
      (with-redefs [q-api/upsert-points
                    (fn [_client & {:keys [points]}] (reset! point (first points)) {:count 1})]
        (let [id (proto/add-entry! s {:id "k1" :type :task :content "do the thing"})]
          (is (= "k1" id) "kanban/structural writes must not brick")
          (is (= vsize (count (:vector @point))))
          (is (false? (proto/supports-semantic-search? s))))))))

;; =============================================================================
;; Capability honesty
;; =============================================================================

(deftest supports-semantic-search-tracks-the-embedder
  (is (false? (proto/supports-semantic-search? (store/create-store {:vector-size vsize})))
      "hardcoded true is the lie that made the dead lane look healthy")
  (is (true? (proto/supports-semantic-search?
              (store/create-store {:vector-size vsize :embedder (unit-embedder)})))))
