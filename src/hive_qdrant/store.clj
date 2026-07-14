(ns hive-qdrant.store
  "QdrantMemoryStore — IMemoryStore implementation backed by clj-qdrant.

   Ships writes through the circuit breaker + queue fail-soft path so an
   unreachable qdrant cluster degrades instead of erroring. Reads on an
   open circuit return queue/degraded-response with actionable tips.

   Entry shape (from hive-mcp.protocols.memory):
     {:id         string (uuid or timestamp-hex)
      :type       keyword|string (e.g. :decision :note :snippet)
      :content    string
      :tags       [string]
      :created-at iso-timestamp string
      :expires-at iso-timestamp string | nil
      :embedding  [float]   ;; optional — only if supports-semantic-search?
      :metadata   map       ;; arbitrary payload}

   The entry is stored as:
     - qdrant point id = (uuid from :id when uuid-shaped, else hash)
     - vector          = :embedding or zero-vector fallback
     - payload         = flattened entry sans :embedding

   Collection name resolves from config :collection-name, default
   \"hive_qdrant_memory\". When not connected, an in-memory fallback atom
   backs reads/writes so tests and offline dev don't need a live cluster."
  (:require [clj-qdrant.api :as q-api]
            [clj-qdrant.client :as q-client]
            [clj-qdrant.schema :as q-schema]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [hive-mcp.protocols.memory :as proto]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.failure :as failure]
            [hive-qdrant.queue :as queue]
            [taoensso.timbre :as log])
  (:import [java.util UUID]))

;; =============================================================================
;; Config + state
;; =============================================================================

(def ^:const default-vector-size 384)
(def ^:const default-collection "hive_qdrant_memory")

(defn- ->uuid-id
  "Coerce a string id to UUID. Non-UUID strings get hashed to a
   deterministic UUID so qdrant's PointIdFactory accepts them."
  [id-str]
  (try
    (UUID/fromString id-str)
    (catch Throwable _
      (UUID/nameUUIDFromBytes (.getBytes (str id-str) "UTF-8")))))

(defn- zero-vec [n] (vec (repeat n 0.0)))

(defn- ->embed-text
  "Text handed to the embedder for an entry. Content may already be a
   serialized string (mem-crud writes JSON) or a native map (kanban/carto
   callers); either way the embedder needs one string."
  [content]
  (cond
    (nil? content)    nil
    (string? content) (when-not (str/blank? content) content)
    :else             (pr-str content)))

(defn- dim-mismatch
  "Guard: qdrant fixes the vector width per collection. A vector of the wrong
   width is not a degraded answer, it is a wrong one — upserting/searching it
   writes or queries garbage. Returns an error map, or nil when the width is
   right. (Mirrors hive-milvus's :embedder/dim-mismatch.)"
  [v vector-size stage]
  (when (and vector-size (seq v) (not= (count v) vector-size))
    {:success? false
     :error    :qdrant/dim-mismatch
     :stage    stage
     :expected vector-size
     :actual   (count v)
     :message  (str "embedding dimension " (count v) " != collection vector-size "
                    vector-size " — refusing to " (name stage)
                    ". Route an embedder whose dimension matches the collection, "
                    "or recreate the collection at the embedder's dimension.")}))

(defonce ^:private unembedded-warned (atom #{}))

(defn- warn-unembedded!
  "One warn per collection: this slot writes structural (zero) vectors."
  [collection]
  (when-not (contains? @unembedded-warned collection)
    (swap! unembedded-warned conj collection)
    (log/warn "hive-qdrant: no embedder injected for collection" collection
              "— points carry placeholder vectors; semantic search is UNAVAILABLE"
              "on this slot (payload/tag reads are unaffected).")))

(defn- resolve-vector
  "The vector for one write or one search — or a loud error.

   {:vector v}         usable: explicit `:embedding`, else `(embedder text)`.
   {:error m}          an embedding lane EXISTS but produced nothing usable, or
                       the vector's width does not match the collection.
   {:no-embedder true} the slot has no embedding lane at all (kanban is
                       structurally addressed: tag/payload reads only).

   The one thing this never does is substitute a zero vector for a failed
   embed. A zero vector scores 0.0 against every other zero vector, so qdrant
   returns arbitrary points and reports success — the exact silence that hid
   this bug. Zeros are only ever written on the explicit :no-embedder path,
   where the caller has declared it does not want semantic search."
  [{:keys [embedding embedder text vector-size stage]}]
  (let [embedded (when (and (empty? embedding) (fn? embedder) (some? text))
                   (try (embedder text)
                        (catch Throwable t {::failed (or (ex-message t) (str t))})))
        v        (if (seq embedding) embedding embedded)]
    (cond
      (and (map? v) (::failed v))
      {:error {:success? false :error :qdrant/embed-failed :stage stage
               :message (::failed v)}}

      (seq v)
      (if-let [e (dim-mismatch v vector-size stage)]
        {:error e}
        {:vector (vec v)})

      (fn? embedder)
      {:error {:success? false :error :qdrant/embed-failed :stage stage
               :message (if (some? text)
                          "embedder returned no vector"
                          "nothing to embed (entry/query carries no content)")}}

      :else {:no-embedder true})))

(defn- entry->point
  "Transform an entry map + its resolved vector into a clj-qdrant point map.
   Tags are stored as a native list (qdrant keyword[] payload field) so they
   can be filtered via match-keyword conditions.

   The vector is resolved by `resolve-vector` at the call site, so this stays
   pure and no zero-vector fallback can hide inside it."
  [{:keys [id] :as entry} vector]
  {:id      (->uuid-id id)
   :vector  vector
   :payload (-> entry
                (dissoc :embedding)
                (update :type #(if (keyword? %) (name %) (str %)))
                (update :tags #(when (seq %) (mapv str %)))
                (->> (into {} (remove (comp nil? val)))))})

(defn- parse-content
  "Deserialize a stringified :content payload back to its native shape.
   Hive-mcp's mem-crud writes content as JSON; entries migrated from
   milvus/proximum may carry the original Clojure-printed map. Try JSON
   first, fall back to EDN, fall back to the raw string when neither
   parses. Non-string / nil content is returned untouched."
  [c]
  (cond
    (nil? c)        nil
    (not (string? c)) c
    ;; JSON object/array shape
    (and (#{\{ \[} (first c))
         (#{\" \: \, \space \] \}} (or (second c) \space)))
    (or (try (json/read-str c :key-fn keyword) (catch Throwable _ nil))
        (try (edn/read-string c)               (catch Throwable _ nil))
        c)
    ;; EDN map/vec where first inside char is `:` keyword start
    (#{\{ \[} (first c))
    (or (try (edn/read-string c)               (catch Throwable _ nil))
        (try (json/read-str c :key-fn keyword) (catch Throwable _ nil))
        c)
    :else c))

(defn- point->entry
  "Extract a clj-qdrant point->map (already-decoded payload) back to entry shape.

   ID resolution: prefer the payload's own :id (the entry's
   stable identifier — e.g. timestamp `20260401224103-05eb4b95`)
   over the qdrant point UUID. The qdrant point id is a deterministic
   UUID hash of the original (see `->uuid-id`); surfacing it would
   break move/delete-by-id flows that look up entries by their
   original timestamp string. Only fall back to the qdrant UUID
   when the payload has no :id (defensive default).

   Content deserialization: qdrant payloads store :content as a
   serialized string (JSON from hive-mcp's add-entry, EDN from
   migrations of Clojure-printed content). Parse it back to a map
   on read so kanban-side predicates (`kanban-task-type?`, status
   readers) see structured content without round-tripping the
   string in caller code.

   Tolerates nil/absent :content — callers (e.g. query-entries with payload
   projection) may request a subset of payload fields that excludes :content,
   in which case the returned entry simply has no :content key."
  [{:keys [id payload]}]
  (let [base (or payload {})
        with-id (cond-> base
                  (and id (not (:id base))) (assoc :id id))]
    (if (contains? with-id :content)
      (update with-id :content parse-content)
      with-id)))

(defn- apply-order-by
  "Sort `entries` by `order-by` tuple `[field direction]`. `field` is a
   keyword on the entry map (e.g. :created). `direction` is :asc or :desc.
   No-op when `order-by` is nil. Qdrant's scroll-points lacks a server-side
   ORDER BY, so callers must over-fetch via :limit to cover the desired top-N."
  [entries order-by]
  (if-let [[field direction] order-by]
    (let [cmp (if (= direction :desc)
                #(compare %2 %1)
                compare)]
      (vec (sort-by field cmp entries)))
    entries))

;; =============================================================================
;; Connection wrapper
;; =============================================================================

(defn- open-client!
  "Open a clj-qdrant client from store config. Returns client-map."
  [{:keys [host port api-key tls?]}]
  (q-client/make {:host    (or host "localhost")
                  :port    (or port 6334)
                  :api-key api-key
                  :tls?    (boolean tls?)}))

(defn- ensure-payload-indexes!
  "Create payload indexes used by query-entries. Idempotent — soft-fails
   when an index already exists. Required fields: tags (keyword), type
   (keyword), project-id (keyword), content-hash (keyword)."
  [client-map collection-name]
  (doseq [[field type] [[:tags :keyword]
                        [:type :keyword]
                        [:project-id :keyword]
                        [:content-hash :keyword]]]
    (try
      (q-schema/payload-index-create! client-map
                                      {:collection collection-name
                                       :field      (name field)
                                       :type       type})
      (catch Throwable t
        (log/debug "payload-index-create! soft-failed for" field
                   "(likely already exists):" (ex-message t))))))

(defn- ensure-collection!
  "Create the collection if missing + ensure payload indexes. Best-effort —
   swallows 'already exists'."
  [client-map {:keys [collection-name vector-size distance]
               :or   {collection-name default-collection
                      vector-size     default-vector-size
                      distance        :cosine}}]
  (try
    (q-schema/collection-create! client-map
                                 {:name        collection-name
                                  :vector-size vector-size
                                  :distance    distance})
    (catch Throwable t
      (log/debug "collection-create! soft-failed (likely already exists):"
                 (ex-message t))
      nil))
  (ensure-payload-indexes! client-map (or collection-name default-collection)))

;; =============================================================================
;; Resilient wrapper — circuit + queue
;; =============================================================================

(defn- resilient
  "Run (thunk) under the circuit breaker. On success, record-success!
   and return the value. On failure, classify + record-failure!. If
   queue-op is provided and failure is transient, enqueue for replay."
  ([thunk] (resilient thunk nil))
  ([thunk queue-op]
   (if-let [gate (circuit/check)]
     ;; circuit open — fail-soft
     (if queue-op
       (queue/enqueue! queue-op)
       gate)
     (try
       (let [result (thunk)]
         (circuit/record-success!)
         result)
       (catch Throwable t
         (circuit/record-failure!)
         (let [f (failure/classify t)]
           (if (and queue-op (failure/transient? f))
             (queue/enqueue! queue-op)
             (failure/->legacy-map f))))))))

;; =============================================================================
;; Fallback in-memory store (used when not connected)
;; =============================================================================

(defn- empty-fallback [] {:entries {}})

;; ── query-entries helpers (SLAP-stratified, railway-oriented) ────────
;;
;; Layered by intent (Onion / CPPB):
;;   • Utility   — defensive coercion (`->type-token`, `->project-id-list`)
;;   • Mechanism — pure data assembly (`build-must-keyword`,
;;                 `build-payload-keys`, `exclude-tagged`,
;;                 `points->ordered-entries`)
;;   • Boundary  — IO seam (`scroll-collection!`, `query-fallback!`)
;;
;; The defrecord method itself stays at the Intent level: a single threaded
;; pipeline. No mixed abstraction levels, no nested let-soup.

(defn- ->type-token
  "Utility: coerce :type opt to qdrant must-keyword token list.
   Handles keyword|string. Returns nil when input is nil so callers can
   omit the filter clause via `some?`."
  [t]
  (when (some? t)
    [(if (keyword? t) (name t) (str t))]))

(defn- ->project-id-list
  "Utility: HCR project-scope reconciliation.
   Plural `:project-ids` wins over singular `:project-id` — descendant
   aggregation needs OR-over-list, and qdrant `must-keyword` on a list
   is OR semantics. Returns nil when neither is supplied so the caller
   omits the filter clause."
  [project-id project-ids]
  (cond
    (seq project-ids)  (mapv str project-ids)
    (some? project-id) [(str project-id)]
    :else              nil))

(defn- build-must-keyword
  "Mechanism: pure opts → qdrant must-keyword filter map.

   Returns a map shaped for `q-api/->filter` consumption:

     {:must-keyword   {:tags [...] :type [...]}    ; AND-of-values per field
      :must-match-any {:project-id [...]}}         ; OR-of-values on field

   `:project-id` is single-valued on each entry, so multi-scope HCR queries
   need MatchAny (OR) semantics via `must-match-any`. Tag filters keep
   AND-of-values semantics: callers asking for tags = [\"kanban\" \"todo\"]
   expect entries carrying BOTH tags. Empty filter (`{}` under each key)
   means match-all on that branch. Each clause is opt-in via predicate so
   partial filters compose without dummy values."
  [{:keys [type tags project-id project-ids]}]
  (let [pid-list (->project-id-list project-id project-ids)
        and-keys (cond-> {}
                   (seq tags)   (assoc :tags (mapv str tags))
                   (some? type) (assoc :type (->type-token type)))
        any-keys (cond-> {}
                   (some? pid-list) (assoc :project-id pid-list))]
    (cond-> {}
      (seq and-keys) (assoc :must-keyword and-keys)
      (seq any-keys) (assoc :must-match-any any-keys))))

(defn- build-payload-keys
  "Mechanism: pure opts → server-side payload projection list.
   Always projects kanban hot-path keys (id, content-hash, expires) so
   move/delete/dedupe paths work without :include-content?. The full
   :content payload is opt-in to keep response size bounded."
  [{:keys [include-content?]}]
  (cond-> ["id" "type" "tags" "project-id"
           "duration" "created-at" "created"
           "updated" "content-hash" "expires"]
    include-content? (conj "content")))

(defn- exclude-tagged
  "Mechanism: pure — drop points whose tags intersect any `exclude-tags`.
   Applied client-side because qdrant lacks negative multi-keyword on
   indexed-list fields without extra config."
  [points exclude-tags]
  (let [excluded (set (map str (or exclude-tags [])))]
    (if (empty? excluded)
      points
      (remove (fn [p]
                (boolean (some excluded (set (get-in p [:payload :tags])))))
              points))))

(defn- points->ordered-entries
  "Mechanism: pure — decode qdrant points to entries and apply :order-by."
  [points order-by]
  (-> (mapv point->entry points)
      (apply-order-by order-by)))

(defn- scroll-collection!
  "Boundary: IO — scroll qdrant with prepared filter + payload projection.
   `filter-opts` is the map shape consumed by `q-api/->filter`:
   `{:must-keyword {...} :must-match-any {...}}`.
   Returns the raw response (`:points` is the relevant key downstream)."
  [client collection-name limit filter-opts payload-keys]
  (q-api/scroll-points client
                       :collection collection-name
                       :limit (int limit)
                       :filter (q-api/->filter filter-opts)
                       :payload-includes payload-keys))

(defn- query-fallback!
  "Boundary: IO — in-memory fallback when no qdrant client is connected.
   Linear take over :entries so degraded mode still satisfies the protocol
   contract (returns ordered seq up to :limit)."
  [fallback-atom limit order-by]
  (-> (->> (:entries @fallback-atom) vals (take limit) vec)
      (apply-order-by order-by)))

;; =============================================================================
;; Store record
;; =============================================================================

(defrecord QdrantMemoryStore
  [config client-atom fallback-atom connected?-atom]

  proto/IMemoryStore

  ;; ---- Connection lifecycle ------------------------------------------------

  (connect! [_this cfg]
    (try
      (let [resolved (merge config cfg)
            client   (open-client! resolved)]
        (ensure-collection! client resolved)
        (reset! client-atom client)
        (reset! connected?-atom true)
        (circuit/force-reset!)
        (log/info "hive-qdrant.store connected" {:host (:host resolved)
                                                  :port (:port resolved)})
        {:success? true :errors [] :backend "qdrant"})
      (catch Throwable t
        (log/warn "hive-qdrant.store connect! failed:" (ex-message t))
        (reset! connected?-atom false)
        {:success? false :errors [(ex-message t)] :backend "qdrant"})))

  (disconnect! [_this]
    (when-let [c @client-atom]
      (try (q-client/close! c) (catch Throwable _ nil)))
    (reset! client-atom nil)
    (reset! connected?-atom false)
    nil)

  (connected? [_this] (boolean @connected?-atom))

  (health-check [_this]
    (let [connected? @connected?-atom]
      {:healthy? connected?
       :status   (if connected? :ok :down)
       :backend  "qdrant"
       :circuit  (:state (circuit/state))}))

  ;; ---- CRUD ----------------------------------------------------------------

  (add-entry! [_this entry]
    ;; Generate a stable timestamp+hex id when the caller didn't supply
    ;; one. Mirror's hive-milvus's behaviour (`(or (:id entry)
    ;; (proto/generate-id))` in record/entry->record-pure). Without
    ;; this fresh writes via hive-mcp's mem-crud get a nil id back,
    ;; tripping the contract guard in `crud.write/do-add!`. Migration
    ;; entries already carry ids; ad-hoc writes (kanban create) do not.
    (let [entry-id (or (:id entry) (proto/generate-id))
          entry+id (assoc entry :id entry-id)]
      (resilient
       (fn []
         (if-let [c @client-atom]
           (let [vs   (:vector-size config default-vector-size)
                 coll (:collection-name config default-collection)
                 rv   (resolve-vector {:embedding   (:embedding entry+id)
                                       :embedder    (:embedder config)
                                       :text        (->embed-text (:content entry+id))
                                       :vector-size vs
                                       :stage       :write})]
             (if-let [e (:error rv)]
               ;; A slot WITH an embedding lane that cannot embed must not
               ;; write a placeholder — that is how 116k unrankable zero
               ;; vectors got into carto-snippets. Fail loud; the caller's
               ;; `(string? raw-id)` guard turns this into a visible error.
               (do (log/warn "hive-qdrant add-entry! refused:" (:message e)
                             {:id entry-id :collection coll})
                   e)
               (let [v     (or (:vector rv)
                               (do (warn-unembedded! coll) (zero-vec vs)))
                     point (entry->point entry+id v)]
                 (q-api/upsert-points c :collection coll :points [point])
                 ;; Contract: return the id string (not the {:success? true ...} map)
                 ;; — matches hive-milvus/hive-chroma so the crud write path's
                 ;; `(string? raw-id)` guard accepts the result.
                 entry-id)))
           ;; fallback in-memory
           (do (swap! fallback-atom assoc-in [:entries entry-id] entry+id)
               entry-id)))
       {:op :add-entry! :id entry-id :args [entry+id]})))

  (get-entry [_this id]
    ;; q-api/get-points returns RAW protobuf points; the clj-qdrant
    ;; `point->map` decoder must run before our `point->entry` shape
    ;; conversion. Without it, destructure on `{:keys [id payload]}`
    ;; in `point->entry` yields nil for both fields, returning an
    ;; entry stripped of tags/content — invisible to `kanban-entry?`
    ;; predicate and surfacing as a misleading "Entry not found" on
    ;; move/delete. `query-entries` doesn't suffer this because
    ;; `scroll-points` already decodes server-side.
    (resilient
     (fn []
       (if-let [c @client-atom]
         (let [res (q-api/get-points c
                                     :collection (:collection-name config default-collection)
                                     :ids [(->uuid-id id)])]
           (some-> (first (:points res)) q-api/point->map point->entry (assoc :id id)))
         (get-in @fallback-atom [:entries id])))))

  (update-entry! [this id updates]
    (let [existing (proto/get-entry this id)
          merged   (merge existing updates {:id id})]
      (proto/add-entry! this merged)))

  (delete-entry! [_this id]
    (resilient
     (fn []
       (if-let [c @client-atom]
         (do (q-api/delete-points c
                                  :collection (:collection-name config default-collection)
                                  :ids [(->uuid-id id)])
             {:success? true :id id})
         (do (swap! fallback-atom update :entries dissoc id)
             {:success? true :id id :backend :fallback})))
     {:op :delete-entry! :id id :args [id]}))

  (query-entries
    ;; Intent layer (SLAP): single-pipeline orchestration.
    ;; Stratified-design layers — Boundary (scroll/fallback), Mechanism
    ;; (filter/projection/decode), Utility (coercion) — live as private
    ;; defns above the defrecord. The method body only chooses live vs
    ;; fallback and threads decoded data through the pure pipeline.
    [_this {:keys [exclude-tags limit order-by] :or {limit 100} :as opts}]
    (let [collection-name (:collection-name config default-collection)]
      (if-let [c @client-atom]
        (resilient
         (fn []
           (-> (scroll-collection! c collection-name limit
                                   (build-must-keyword opts)
                                   (build-payload-keys opts))
               :points
               (exclude-tagged exclude-tags)
               (points->ordered-entries order-by))))
        (query-fallback! fallback-atom limit order-by))))

  ;; ---- Semantic search -----------------------------------------------------

  (search-similar [_this query-text {:keys [limit embedding exclude-tags]
                                     :or   {limit 10}
                                     :as   opts}]
    (resilient
     (fn []
       (if-let [c @client-atom]
         (let [vs (:vector-size config default-vector-size)
               rv (resolve-vector {:embedding   embedding
                                   :embedder    (:embedder config)
                                   :text        (->embed-text query-text)
                                   :vector-size vs
                                   :stage       :search})]
           (cond
             (:error rv) (:error rv)

             ;; No embedding lane => there is no semantic search on this slot.
             ;; Saying so is the whole point: the previous zero-vector query
             ;; returned arbitrary points at score 0.0 and reported :success?.
             (:no-embedder rv)
             {:success? false
              :error    :qdrant/no-embedder
              :count    0
              :results  []
              :message  (str "no embedder configured for collection "
                             (:collection-name config default-collection)
                             " — semantic search unavailable on this slot")}

             :else
             (let [r    (q-api/search-points
                         c
                         :collection (:collection-name config default-collection)
                         :vector (:vector rv)
                         :limit limit
                         ;; :project-ids / :type were silently discarded before —
                         ;; even a working vector search returned cross-project
                         ;; hits. Same filter builder query-entries uses.
                         :filter (q-api/->filter (build-must-keyword opts))
                         :with-payload? true)
                   ;; search-points hands back DECODED {:id :payload :score}
                   ;; maps. Shaping them to entries here is what lets
                   ;; granular/normalize-semantic take its (map? row) branch and
                   ;; recover :qn/:tags/:content — raw ScoredPoints degraded to
                   ;; :opaque rows that semantic-grep's (filter :qn) then dropped.
                   rows (-> (:results r)
                            (exclude-tagged exclude-tags)
                            (->> (mapv (fn [p]
                                         (assoc (point->entry p) :score (:score p))))))]
               {:success? true :count (count rows) :results rows})))
         {:success? true :count 0 :results []}))))

  (supports-semantic-search? [_this]
    ;; Honest capability: true iff a real embedder is wired. Hardcoding `true`
    ;; while every vector was zeros is what made the broken lane look healthy.
    (boolean (fn? (:embedder config))))

  ;; ---- Expiration ----------------------------------------------------------

  (cleanup-expired! [_this]
    ;; No scroll yet — report no-op.
    {:success? true :deleted 0})

  (entries-expiring-soon [_this _days _opts] [])

  ;; ---- Duplicate detection -------------------------------------------------

  (find-duplicate [_this type content-hash {:keys [project-id]}]
    ;; Server-side scroll on indexed :content-hash payload (idx created in
    ;; ensure-payload-indexes!). Without this, every cartography rescan saw
    ;; every form as new — fresh ids → orphaned qdrant points → KG edges
    ;; pointing at stale UUIDs (kanban 20260503210515-306bfe41).
    ;;
    ;; Read-only + idempotent — never queues, never throws. On scroll failure
    ;; (transport, schema mismatch) returns nil so callers fall through to
    ;; fresh-insert; the worst case degrades to pre-fix behaviour.
    (if-let [c @client-atom]
      (try
        (let [and-keys (cond-> {:content-hash [(str content-hash)]}
                         (some? type) (assoc :type (->type-token type)))
              ;; Singleton :project-id can ride the same MatchAny path —
              ;; a single-element OR-list is equivalent to the legacy
              ;; per-value matchKeyword loop and keeps semantics uniform.
              any-keys (cond-> {}
                         (some? project-id) (assoc :project-id [(str project-id)]))
              filter-opts (cond-> {:must-keyword and-keys}
                            (seq any-keys) (assoc :must-match-any any-keys))
              resp         (scroll-collection!
                            c
                            (:collection-name config default-collection)
                            1
                            filter-opts
                            ["id" "type" "tags" "project-id" "content-hash"])]
          (some-> (first (:points resp)) point->entry))
        (catch Throwable t
          (log/debug "find-duplicate scroll failed; returning nil:" (ex-message t))
          nil))
      ;; Fallback in-memory mode: linear scan.
      (first
       (filter #(= content-hash (:content-hash %))
               (vals (:entries @fallback-atom))))))

  ;; ---- Store management ----------------------------------------------------

  (store-status [_this]
    {:backend         "qdrant"
     :connected?      (boolean @connected?-atom)
     :collection-name (:collection-name config default-collection)
     :vector-size     (:vector-size config default-vector-size)
     :circuit         (:state (circuit/state))
     :queue           (queue/stats)})

  (reset-store! [_this]
    (reset! fallback-atom (empty-fallback))
    (when-let [c @client-atom]
      (try
        (q-schema/collection-recreate! c
                                       {:name        (:collection-name config default-collection)
                                        :vector-size (:vector-size config default-vector-size)
                                        :distance    :cosine})
        (catch Throwable t
          (log/warn "reset-store! recreate failed:" (ex-message t)))))
    {:success? true}))

;; =============================================================================
;; IMemoryStoreLiveness — cross-store resilience seam
;; =============================================================================
;;
;; Qdrant resilience model differs from Milvus: passive circuit-breaker +
;; in-memory fallback rather than an active heal loop. The protocol
;; implementation here is a thin adapter:
;;
;; -probe!            — true iff connected?-atom is set; no real RPC. Cheap
;;                      and accurate for qdrant since `connect!` flips the
;;                      flag based on a successful schema check.
;; -kick-reconnect!   — close existing client, attempt reconnect via the
;;                      stored `config`. Idempotent: re-running connect!
;;                      with the same config is a no-op when already up.
;; -await-reconnect!  — poll `connected?-atom` up to budget-ms, 100ms cadence.
;;
;; The fallback-atom path means qdrant degrades gracefully under load even
;; without a heal loop; the protocol seam is here primarily so the
;; resilience layer can drive recovery on transient transport failures
;; (NAT idle-timeout, ingress flap) without importing qdrant internals.

(require '[hive-mcp.protocols.memory-liveness :as liveness])

(extend-protocol liveness/IMemoryStoreLiveness
  QdrantMemoryStore
  (-probe! [this]
    (boolean @(:connected?-atom this)))
  (-kick-reconnect! [this]
    (proto/disconnect! this)
    (try
      (proto/connect! this (:config this))
      (catch Throwable t
        (log/warn "qdrant kick-reconnect! connect attempt failed:" (ex-message t))))
    nil)
  (-await-reconnect! [this budget-ms]
    (let [deadline (+ (System/currentTimeMillis) budget-ms)
          a (:connected?-atom this)]
      (loop []
        (cond
          @a true
          (>= (System/currentTimeMillis) deadline) (boolean @a)
          :else (do (Thread/sleep 100) (recur)))))))

;; =============================================================================
;; Factory
;; =============================================================================

(defn create-store
  "Construct a QdrantMemoryStore. Does NOT open a connection —
   call (proto/connect! store cfg) to activate."
  ([] (create-store {}))
  ([config]
   (->QdrantMemoryStore (merge {:collection-name default-collection
                                :vector-size     default-vector-size
                                :distance        :cosine}
                               config)
                        (atom nil)          ;; client-atom
                        (atom (empty-fallback)) ;; fallback-atom
                        (atom false))))     ;; connected?-atom
