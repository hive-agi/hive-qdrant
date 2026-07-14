(ns hive-qdrant.migrate
  "Chroma / Milvus → qdrant migration helper.

   Minimal skeleton mirroring hive-milvus.migrate intent but with the
   extraction step factored out to a pluggable source fn so this ns has
   no hard dep on chroma-clj / milvus-clj. Real migrations bind
   :source-fn to a concrete extractor.

   Usage:
     (require '[hive-qdrant.store :as store])
     (require '[hive-qdrant.migrate :as mig])

     (def src-store (load-chroma-or-milvus))
     (def qdrant    (store/create-store {:host \"localhost\" :port 6334}))
     (hive-spi.memory.ports/connect! qdrant {})

     (mig/sync!
       {:source-fn  (fn [] (fetch-all-from src-store))
        :target     qdrant
        :batch-size 500}))"
  (:require [hive-spi.memory.ports :as proto]
            [taoensso.timbre :as log]))

;; =============================================================================
;; Phases
;; =============================================================================

(defn- extract
  "Call source-fn → collection of entries."
  [{:keys [source-fn] :as _opts}]
  (assert source-fn "sync! requires :source-fn")
  (source-fn))

(defn- transform
  "Normalize each entry to the qdrant store's expected shape."
  [entries]
  (vec
   (for [e entries]
     (-> e
         (update :id    #(or % (str (random-uuid))))
         (update :type  #(or % :note))
         (update :tags  #(or % []))))))

(defn- load-batch!
  "Upsert one batch into the target store."
  [target batch]
  (reduce (fn [{:keys [ok fail] :as acc} entry]
            (try
              (let [r (proto/add-entry! target entry)]
                (if (:success? r)
                  (assoc acc :ok (inc ok))
                  (assoc acc :fail (inc fail))))
              (catch Throwable t
                (log/warn "migrate load failed:" (ex-message t))
                (assoc acc :fail (inc fail)))))
          {:ok 0 :fail 0}
          batch))

;; =============================================================================
;; Public API
;; =============================================================================

(defn sync!
  "Migrate entries from :source-fn into :target store, batched.

   Options:
     :source-fn  REQUIRED. 0-arity fn returning a sequence of entry maps.
     :target     REQUIRED. An IMemoryStore (already connected).
     :batch-size Default 500. Upsert batch size.
     :dry-run?   When truthy, run extract+transform only; skip writes.

   Returns {:extracted N :transformed N :loaded-ok N :loaded-fail N
            :batches N :dry-run? bool}."
  [{:keys [target batch-size dry-run?]
    :or   {batch-size 500}
    :as   opts}]
  (assert target "sync! requires :target IMemoryStore")
  (log/info "hive-qdrant.migrate sync! starting — batch-size" batch-size)
  (let [raw         (extract opts)
        extracted   (count raw)
        entries     (transform raw)
        transformed (count entries)]
    (if dry-run?
      {:extracted   extracted
       :transformed transformed
       :loaded-ok   0
       :loaded-fail 0
       :batches     0
       :dry-run?    true}
      (let [batches (partition-all batch-size entries)
            totals  (reduce (fn [acc batch]
                              (let [{:keys [ok fail]} (load-batch! target batch)]
                                (-> acc
                                    (update :loaded-ok   + ok)
                                    (update :loaded-fail + fail)
                                    (update :batches     inc))))
                            {:loaded-ok 0 :loaded-fail 0 :batches 0}
                            batches)]
        (merge {:extracted   extracted
                :transformed transformed
                :dry-run?    false}
               totals)))))

(defn verify
  "Spot-check N random ids round-trip correctly. Returns
   {:checked N :ok N :missing [id...]}."
  [{:keys [target ids]}]
  (assert target "verify requires :target")
  (let [results (for [id ids]
                  (try
                    [id (some? (proto/get-entry target id))]
                    (catch Throwable _ [id false])))]
    {:checked (count ids)
     :ok      (count (filter second results))
     :missing (vec (map first (remove second results)))}))
