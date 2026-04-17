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

(defn- entry->point
  "Transform an entry map into a clj-qdrant point map.
   Tags are stored as a native list (qdrant keyword[] payload field) so they
   can be filtered via match-keyword conditions."
  [{:keys [id embedding] :as entry} vector-size]
  {:id      (->uuid-id id)
   :vector  (or embedding (zero-vec vector-size))
   :payload (-> entry
                (dissoc :embedding)
                (update :type #(if (keyword? %) (name %) (str %)))
                (update :tags #(when (seq %) (mapv str %)))
                (->> (into {} (remove (comp nil? val)))))})

(defn- point->entry
  "Extract a clj-qdrant point->map (already-decoded payload) back to entry shape."
  [{:keys [id payload]}]
  (cond-> (or payload {})
    id (assoc :id id)))

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
    (resilient
     (fn []
       (if-let [c @client-atom]
         (let [vs (:vector-size config default-vector-size)
               point (entry->point entry vs)]
           (q-api/upsert-points c
                                :collection (:collection-name config default-collection)
                                :points [point])
           {:success? true :id (:id entry)})
         ;; fallback in-memory
         (do (swap! fallback-atom assoc-in [:entries (:id entry)] entry)
             {:success? true :id (:id entry) :backend :fallback})))
     {:op :add-entry! :id (:id entry) :args [entry]}))

  (get-entry [_this id]
    (resilient
     (fn []
       (if-let [c @client-atom]
         (let [res (q-api/get-points c
                                     :collection (:collection-name config default-collection)
                                     :ids [(->uuid-id id)])]
           (some-> (first (:points res)) point->entry (assoc :id id)))
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

  (query-entries [_this {:keys [type project-id tags exclude-tags limit]
                         :or   {limit 100}
                         :as   _opts}]
    (if-let [c @client-atom]
      (resilient
       (fn []
         (let [must-keyword (cond-> {}
                              (seq tags)         (assoc :tags (mapv str tags))
                              (and type (some? type))
                              (assoc :type [(if (keyword? type) (name type) (str type))])
                              (and project-id (some? project-id))
                              (assoc :project-id [(str project-id)]))
               flt          (q-api/->filter {:must-keyword must-keyword})
               resp         (q-api/scroll-points c
                                                 :collection (:collection-name config default-collection)
                                                 :limit (int limit)
                                                 :filter flt)
               points       (:points resp)
               ;; Apply :exclude-tags client-side (qdrant lacks negative
               ;; multi-keyword on indexed-list fields without extra config).
               excluded     (set (map str (or exclude-tags [])))
               filtered     (if (empty? excluded)
                              points
                              (remove (fn [p]
                                        (let [pt (set (get-in p [:payload :tags]))]
                                          (boolean (some excluded pt))))
                                      points))]
           (mapv point->entry filtered))))
      (->> (:entries @fallback-atom)
           vals
           (take limit)
           vec)))

  ;; ---- Semantic search -----------------------------------------------------

  (search-similar [_this _query-text {:keys [limit embedding] :or {limit 10}}]
    (resilient
     (fn []
       (if-let [c @client-atom]
         (let [vs (:vector-size config default-vector-size)
               v  (or embedding (zero-vec vs))
               r  (q-api/search-points c
                                       :collection (:collection-name config default-collection)
                                       :vector v
                                       :limit limit)]
           {:success? true :count (:count r) :results (:results r)})
         {:success? true :count 0 :results []}))))

  (supports-semantic-search? [_this] true)

  ;; ---- Expiration ----------------------------------------------------------

  (cleanup-expired! [_this]
    ;; No scroll yet — report no-op.
    {:success? true :deleted 0})

  (entries-expiring-soon [_this _days _opts] [])

  ;; ---- Duplicate detection -------------------------------------------------

  (find-duplicate [_this _type content-hash _opts]
    ;; Fallback-only: scan payloads for matching :content-hash.
    (when-not @client-atom
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
