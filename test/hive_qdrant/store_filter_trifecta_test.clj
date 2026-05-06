(ns hive-qdrant.store-filter-trifecta-test
  "Trifecta tests for the SLAP-stratified query-entries helpers.

   Each pure helper extracted from `hive-qdrant.store/QdrantMemoryStore`
   gets a golden + property + mutation pinning so the contract is
   impossible to silently regress. Pattern follows
   `hive-mcp.tools.kanban.list-filter-trifecta-test` — multi-arg
   subjects collapsed into unary `run-*` case-map adapters.

   Subject fns under test (private in `hive-qdrant.store`):
     - ->type-token            — keyword|string|nil → list-of-string|nil
     - ->project-id-list       — HCR plural-vs-singular reconciliation
     - build-must-keyword      — opts → qdrant must-keyword filter
     - build-payload-keys      — opts → server-side projection list
     - exclude-tagged          — points + exclude-tags → filtered points

   `points->ordered-entries` is exercised end-to-end via the
   `query-entries` integration tests; its decode path depends on the
   point/payload shape and does not benefit from a pure trifecta in
   isolation.

   2026-05-04 origin: regression where qdrant `query-entries` silently
   dropped `:project-ids`, leaking cross-tree scopes through HCR
   descendant aggregation. The trifecta pins each helper so the
   plural-wins-over-singular contract cannot drift.

   Mutations are designed so each FAILS at least one golden case —
   tests that pass alongside a real bug are the failure mode the
   mutation facet exists to prevent."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [hive-test.trifecta :refer [deftrifecta]]
            [hive-qdrant.store :as store]))

;; =============================================================================
;; Adapter helpers — collapse multi-arg subjects into unary case-map fns
;; Private vars referenced via #'ns/name so trifecta `var-sym` resolves.
;; =============================================================================

(defn run-type-token
  "Adapter: case-map → list-or-nil. Trifecta-friendly subject for
   the private `->type-token` helper."
  [{:keys [t]}]
  (#'store/->type-token t))

(defn run-project-id-list
  "Adapter: case-map → list-or-nil. Tests HCR plural-wins-over-singular."
  [{:keys [pid pids]}]
  (#'store/->project-id-list pid pids))

(defn run-build-must-keyword
  "Adapter: opts-map → filter-map. Subject already unary; alias kept
   for naming symmetry with the other adapters."
  [opts]
  (#'store/build-must-keyword opts))

(defn run-build-payload-keys
  "Adapter: opts-map → string vec. Subject already unary."
  [opts]
  (#'store/build-payload-keys opts))

(defn run-exclude-tagged
  "Adapter: case-map → vec of points. Wraps the seq output in vec for
   stable golden snapshotting."
  [{:keys [points excl]}]
  (vec (#'store/exclude-tagged points excl)))

;; =============================================================================
;; 1. ->type-token — keyword|string|nil → list-of-string|nil
;; =============================================================================

(deftrifecta type-token-contract
  hive-qdrant.store-filter-trifecta-test/run-type-token
  {:golden-path "test/golden/qdrant/filter/type-token.edn"
   :cases       {:nil      {:t nil}
                 :keyword  {:t :note}
                 :string   {:t "note"}
                 :empty    {:t ""}
                 :ns-keyword {:t :foo/bar}}
   :gen         (gen/let [t (gen/one-of [(gen/return nil)
                                         gen/keyword
                                         gen/string-alphanumeric])]
                  {:t t})
   :pred        (fn [out] (or (nil? out)
                              (and (vector? out)
                                   (= 1 (count out))
                                   (string? (first out)))))
   :num-tests   200
   :mutations   [["always-nil"     (fn [_] nil)]
                 ["drop-vector"    (fn [{:keys [t]}] (when t (str t)))]
                 ["double-wrapped" (fn [{:keys [t]}] (when t [[t]]))]]})

;; =============================================================================
;; 2. ->project-id-list — HCR plural-vs-singular reconciliation
;; =============================================================================

(deftrifecta project-id-list-contract
  hive-qdrant.store-filter-trifecta-test/run-project-id-list
  {:golden-path "test/golden/qdrant/filter/project-id-list.edn"
   :cases       {:both-nil           {:pid nil :pids nil}
                 :singular-only      {:pid "hive" :pids nil}
                 :plural-only        {:pid nil :pids ["hive-mcp" "hive-knowledge"]}
                 :plural-wins        {:pid "hive" :pids ["hive-mcp" "hive-knowledge"]}
                 :empty-plural-falls {:pid "hive" :pids []}
                 :single-plural      {:pid nil :pids ["solo"]}
                 ;; Non-string inputs pin the str-coercion contract — without
                 ;; one such case, the `drop-stringify` mutation is identity
                 ;; over string inputs and survives the suite.
                 :keyword-pid        {:pid :hive :pids nil}
                 :numeric-pids       {:pid nil :pids [42 99]}}
   :gen         (gen/let [pid  (gen/one-of [(gen/return nil) gen/string-alphanumeric])
                          pids (gen/one-of [(gen/return nil)
                                            (gen/return [])
                                            (gen/vector gen/string-alphanumeric 1 4)])]
                  {:pid pid :pids pids})
   :pred        (fn [out] (or (nil? out)
                              (and (vector? out)
                                   (every? string? out))))
   :num-tests   200
   :mutations   [["always-nil"      (fn [_] nil)]
                 ["singular-wins"   (fn [{:keys [pid pids]}]
                                      (cond (some? pid) [(str pid)]
                                            (seq pids)  (mapv str pids)
                                            :else       nil))]
                 ["concat-both"     (fn [{:keys [pid pids]}]
                                      (let [a (when pid [(str pid)])
                                            b (mapv str (or pids []))]
                                        (vec (concat a b))))]
                 ["drop-stringify"  (fn [{:keys [pid pids]}]
                                      (cond (seq pids)  (vec pids)
                                            (some? pid) [pid]
                                            :else       nil))]]})

;; =============================================================================
;; 3. build-must-keyword — opts → qdrant filter map
;; =============================================================================

(deftrifecta build-must-keyword-contract
  hive-qdrant.store-filter-trifecta-test/run-build-must-keyword
  {:golden-path "test/golden/qdrant/filter/build-must-keyword.edn"
   :cases       {:empty           {}
                 :tags-only       {:tags ["kanban" "todo"]}
                 :type-only       {:type :note}
                 :pid-singular    {:project-id "hive"}
                 :pid-plural      {:project-ids ["hive-mcp" "hive-knowledge"]}
                 :hcr-plural-wins {:project-id "hive"
                                   :project-ids ["hive-mcp" "hive-knowledge"]}
                 :empty-tags-skip {:tags []}
                 :all-clauses     {:type :note
                                   :tags ["kanban"]
                                   :project-ids ["hive-mcp"]}}
   :gen         (gen/let [tags  (gen/vector gen/string-alphanumeric 0 3)
                          t     (gen/one-of [(gen/return nil) gen/keyword])
                          pid   (gen/one-of [(gen/return nil) gen/string-alphanumeric])
                          pids  (gen/one-of [(gen/return nil)
                                             (gen/vector gen/string-alphanumeric 0 3)])]
                  (cond-> {}
                    (seq tags)        (assoc :tags tags)
                    (some? t)         (assoc :type t)
                    (some? pid)       (assoc :project-id pid)
                    (some? pids)      (assoc :project-ids pids)))
   :pred        (fn [out]
                  (and (map? out)
                       (every? #{:tags :type :project-id} (keys out))
                       (every? #(or (vector? %) (sequential? %)) (vals out))))
   :num-tests   200
   :mutations   [["always-empty"        (fn [_] {})]
                 ["singular-pid-wins"   (fn [{:keys [type tags project-id project-ids]}]
                                          (cond-> {}
                                            (seq tags)         (assoc :tags (mapv str tags))
                                            (some? type)       (assoc :type [(if (keyword? type)
                                                                               (name type)
                                                                               (str type))])
                                            (some? project-id) (assoc :project-id [(str project-id)])
                                            (and (empty? project-id) (seq project-ids))
                                            (assoc :project-id (mapv str project-ids))))]
                 ["wrong-pid-key"       (fn [{:keys [project-id project-ids] :as opts}]
                                          (cond-> {}
                                            (seq (:tags opts))    (assoc :tags (mapv str (:tags opts)))
                                            (some? (:type opts))  (assoc :type [(name (:type opts))])
                                            (seq project-ids)     (assoc :project-ids (mapv str project-ids))
                                            (and (empty? project-ids) (some? project-id))
                                            (assoc :project-id [(str project-id)])))]
                 ["drop-tag-coercion"   (fn [opts]
                                          (cond-> {}
                                            (seq (:tags opts)) (assoc :tags (:tags opts))))]]})

;; =============================================================================
;; 4. build-payload-keys — opts → server-side projection list
;; =============================================================================

(def ^:private base-payload-keys
  ["id" "type" "tags" "project-id"
   "duration" "created-at" "created"
   "updated" "content-hash" "expires"])

(deftrifecta build-payload-keys-contract
  hive-qdrant.store-filter-trifecta-test/run-build-payload-keys
  {:golden-path "test/golden/qdrant/filter/build-payload-keys.edn"
   :cases       {:default          {}
                 :explicit-false   {:include-content? false}
                 :include-content  {:include-content? true}
                 :other-keys-noop  {:project-ids ["x"] :include-content? false}}
   :gen         (gen/let [flag (gen/one-of [(gen/return nil)
                                            (gen/return false)
                                            (gen/return true)])]
                  (cond-> {} (some? flag) (assoc :include-content? flag)))
   :pred        (fn [out]
                  (and (vector? out)
                       (every? string? out)
                       (every? #(some #{%} out) ["id" "content-hash" "expires"])))
   :num-tests   100
   :mutations   [["drop-id"            (fn [opts]
                                         (cond-> (filterv (complement #{"id"}) base-payload-keys)
                                           (:include-content? opts) (conj "content")))]
                 ["always-with-content" (fn [_] (conj base-payload-keys "content"))]
                 ["drop-content-hash"   (fn [opts]
                                          (cond-> (filterv (complement #{"content-hash"}) base-payload-keys)
                                            (:include-content? opts) (conj "content")))]]})

;; =============================================================================
;; 5. exclude-tagged — drop points whose tags intersect exclude-tags
;; =============================================================================

(def ^:private sample-points
  [{:id 1 :payload {:tags ["a" "b"]}}
   {:id 2 :payload {:tags ["c"]}}
   {:id 3 :payload {:tags []}}
   {:id 4 :payload {:tags ["a" "d"]}}])

(deftrifecta exclude-tagged-contract
  hive-qdrant.store-filter-trifecta-test/run-exclude-tagged
  {:golden-path "test/golden/qdrant/filter/exclude-tagged.edn"
   :cases       {:no-exclude       {:points sample-points :excl []}
                 :nil-exclude      {:points sample-points :excl nil}
                 :single-tag       {:points sample-points :excl ["a"]}
                 :multi-tag        {:points sample-points :excl ["a" "c"]}
                 :no-match         {:points sample-points :excl ["zzz"]}
                 :empty-points     {:points [] :excl ["a"]}
                 :all-empty        {:points [] :excl []}}
   :gen         (gen/let [n     (gen/choose 0 5)
                          excl  (gen/vector gen/string-alphanumeric 0 3)
                          pts   (gen/vector
                                  (gen/let [tags (gen/vector gen/string-alphanumeric 0 4)
                                            id   gen/nat]
                                    {:id id :payload {:tags tags}})
                                  n)]
                  {:points pts :excl excl})
   :pred        (fn [out] (and (vector? out)
                               (every? #(map? %) out)
                               (every? #(get-in % [:payload :tags]) out)))
   :num-tests   150
   :mutations   [["identity"     (fn [{:keys [points]}] (vec points))]
                 ["drop-all"     (fn [_] [])]
                 ["inverse"      (fn [{:keys [points excl]}]
                                   (let [excluded (set (map str (or excl [])))]
                                     (if (empty? excluded)
                                       []
                                       (vec (filter (fn [p]
                                                      (boolean (some excluded
                                                                     (set (get-in p [:payload :tags])))))
                                                    points)))))]]})

;; =============================================================================
;; Cross-cutting invariant: HCR plural-wins-over-singular at integration
;; =============================================================================

(deftest hcr-plural-wins-end-to-end
  (testing "build-must-keyword honors plural :project-ids over singular :project-id"
    (let [out (#'store/build-must-keyword
                {:project-id "hive"
                 :project-ids ["hive-mcp" "hive-knowledge"]})]
      (is (= ["hive-mcp" "hive-knowledge"] (:project-id out))
          "plural list, NOT [\"hive\"] — descendant bundle = OR-over-list"))))
