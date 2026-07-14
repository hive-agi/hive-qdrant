(ns hive-qdrant.queue
  "Global in-memory write queue + read fail-soft shim for qdrant outages.

   Single JVM-wide singleton shared across all projects and agents.
   When the circuit breaker opens, mutating protocol calls enqueue here
   instead of failing; reads return a degraded response. On :closed
   transition, drain! flushes the queue via a single-writer core.async
   pipeline, coalescing by (op,id) to keep only the latest mutation.

   Mirrors hive-milvus.queue without the hive-weave dep — uses
   core.async bounded channel for single-writer serialization."
  (:require [clojure.core.async :as a]
            [hive-spi.memory.ports :as proto]
            [taoensso.timbre :as log])
  (:import [clojure.lang PersistentQueue]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def ^:const max-size
  "Soft cap on queue depth before rejecting writes."
  100000)

(def ^:const drain-timeout-ms
  "Per-item timeout during drain."
  15000)

;; =============================================================================
;; State
;; =============================================================================

(def ^:private the-queue (atom PersistentQueue/EMPTY))
(def ^:private draining? (atom false))

(def ^:private metrics
  (atom {:enqueued      0
         :rejected-full 0
         :drained-ok    0
         :drained-fail  0
         :drain-passes  0}))

(defn size [] (count @the-queue))
(defn full? [] (>= (size) max-size))
(defn drain-running? [] @draining?)

(defn stats []
  (assoc @metrics :depth (size) :max-size max-size :draining? (drain-running?)))

(defn clear! []
  (reset! the-queue PersistentQueue/EMPTY)
  :cleared)

;; =============================================================================
;; Enqueue
;; =============================================================================

(defn- now-ms [] (System/currentTimeMillis))

(defn enqueue!
  "Append op descriptor {:op kw :id str :args [..]}. Returns
   {:success? true :queued? true :depth N :tips [..]} on accept,
   or {:success? false :error :queue-full :depth N :tips [..]} on overflow."
  [op]
  (let [op*       (assoc op :ts (now-ms))
        [old new] (swap-vals! the-queue
                              (fn [q]
                                (if (>= (count q) max-size)
                                  q
                                  (conj q op*))))
        accepted? (> (count new) (count old))]
    (if accepted?
      (do (swap! metrics update :enqueued inc)
          {:success? true
           :queued?  true
           :depth    (count new)
           :tips     [(str "Write queued for replay (depth " (count new)
                           "/" max-size ").")
                      "Will drain automatically when qdrant reconnects."]})
      (do (swap! metrics update :rejected-full inc)
          (log/warn "hive-qdrant.queue overflow, rejecting op" (:op op))
          {:success? false
           :error    :queue-full
           :depth    (count new)
           :tips     [(str "Write queue full at " max-size " — qdrant unreachable.")
                      "Operation NOT recorded. Retry after circuit recovers."]}))))

;; =============================================================================
;; Read fail-soft
;; =============================================================================

(defn degraded-response
  "Build a read fail-soft response carrying actionable tips + queue depth."
  ([] (degraded-response {}))
  ([{:keys [retry-after-ms reconnect-eta-ms operation extra-tips]
     :or   {retry-after-ms 5000}}]
   (let [depth (size)]
     {:success?         false
      :degraded?        true
      :backend          "qdrant"
      :reconnecting?    true
      :retry-after-ms   retry-after-ms
      :reconnect-eta-ms reconnect-eta-ms
      :queue-depth      depth
      :operation        operation
      :tips             (vec
                         (concat
                          ["qdrant unreachable — degraded response (no data)."
                           (format "Retry in ~%.1fs once the circuit recovers."
                                   (/ (double retry-after-ms) 1000.0))
                           (if reconnect-eta-ms
                             (format "Reconnect ETA: ~%.1fs."
                                     (/ (double reconnect-eta-ms) 1000.0))
                             "Reconnect ETA unknown.")
                           (str "Pending writes in queue: " depth)]
                          extra-tips))})))

;; =============================================================================
;; Coalesce
;; =============================================================================

(defn- coalesce-key [op]
  (if-let [id (:id op)]
    [(:op op) id]
    [:singleton (:op op)]))

(defn coalesce
  "Reduce ops to the latest mutation per (op, id), preserving first-seen order."
  [ops]
  (let [indexed    (map-indexed vector ops)
        first-seen (persistent!
                    (reduce (fn [m [i op]]
                              (let [k (coalesce-key op)]
                                (if (contains? m k) m (assoc! m k i))))
                            (transient {})
                            indexed))
        latest     (persistent!
                    (reduce (fn [m op] (assoc! m (coalesce-key op) op))
                            (transient {})
                            ops))]
    (->> (vals latest)
         (sort-by #(get first-seen (coalesce-key %)))
         vec)))

;; =============================================================================
;; Drain (single-writer via core.async)
;; =============================================================================

(defn- run-op
  "Execute one queued op via dispatch-fn. Returns ::ok or ::failed."
  [dispatch-fn op]
  (try
    (dispatch-fn op)
    ::ok
    (catch Throwable e
      (log/warn "hive-qdrant.queue drain op failed:" (:op op) "/" (:id op)
                "—" (ex-message e))
      ::failed)))

(defn- drain-pass!
  "One drain iteration. Single-writer: ops run sequentially through a
   bounded core.async channel, bounded by drain-timeout-ms per item."
  [dispatch-fn]
  (let [[snapshot _] (swap-vals! the-queue (constantly PersistentQueue/EMPTY))
        batch        (coalesce (vec snapshot))
        _            (log/info "hive-qdrant.queue drain pass: coalesced"
                               (count snapshot) "→" (count batch))
        out          (a/chan (max 1 (count batch)))
        _            (a/go
                       (doseq [op batch]
                         (let [r (a/alt!!
                                   (a/timeout drain-timeout-ms) ::failed
                                   (a/go (run-op dispatch-fn op)) ([v] v))]
                           (a/>! out [op r])))
                       (a/close! out))
        results      (loop [acc []]
                       (if-let [pair (a/<!! out)]
                         (recur (conj acc pair))
                         acc))
        failed-ops   (into [] (keep (fn [[op r]] (when (not= r ::ok) op))) results)
        ok-cnt       (- (count batch) (count failed-ops))]
    (when (seq failed-ops)
      (swap! the-queue #(reduce conj % failed-ops)))
    (swap! metrics (fn [m]
                     (-> m
                         (update :drained-ok + ok-cnt)
                         (update :drained-fail + (count failed-ops))
                         (update :drain-passes inc))))
    {:attempted  (count batch)
     :succeeded  ok-cnt
     :failed     (count failed-ops)
     :failed-ops failed-ops}))

(defn drain!
  "Drain the queue until empty, circuit reopens, or a pass makes zero
   forward progress. Options: :dispatch-fn REQUIRED, :circuit-open? pred."
  [{:keys [dispatch-fn circuit-open?]
    :or   {circuit-open? (constantly false)}}]
  (assert dispatch-fn "drain! requires :dispatch-fn")
  (if-not (compare-and-set! draining? false true)
    {:result :already-running :attempted 0 :succeeded 0 :failed 0 :passes 0}
    (try
      (log/info "hive-qdrant.queue drain started — depth" (size))
      (loop [tot {:attempted 0 :succeeded 0 :failed 0 :passes 0}]
        (cond
          (zero? (size))
          (do (log/info "hive-qdrant.queue drain complete —" tot)
              (assoc tot :result :drained))

          (circuit-open?)
          (do (log/warn "hive-qdrant.queue drain aborted — circuit reopened, depth"
                        (size))
              (assoc tot :result :circuit-reopened))

          :else
          (let [pass (drain-pass! dispatch-fn)
                tot' {:attempted (+ (:attempted tot) (:attempted pass))
                      :succeeded (+ (:succeeded tot) (:succeeded pass))
                      :failed    (+ (:failed tot) (:failed pass))
                      :passes    (inc (:passes tot))}]
            (if (and (pos? (:attempted pass)) (zero? (:succeeded pass)))
              (do (log/warn "hive-qdrant.queue drain pass zero progress — backing off")
                  (assoc tot' :result :all-failed))
              (recur tot')))))
      (finally
        (reset! draining? false)))))

;; =============================================================================
;; Store integration helper
;; =============================================================================

(defn make-store-dispatch
  "Build a drain dispatch-fn bound to a concrete store. Each queued op
   becomes a direct proto/* call against the store. Unknown :op throws."
  [store]
  (fn dispatch [{:keys [op args]}]
    (case op
      :add-entry!       (proto/add-entry! store (first args))
      :update-entry!    (proto/update-entry! store (first args) (second args))
      :delete-entry!    (proto/delete-entry! store (first args))
      :cleanup-expired! (proto/cleanup-expired! store)
      (throw (ex-info "hive-qdrant.queue: unknown queued op"
                      {:op op :args args})))))
