(ns hive-qdrant.queue
  "Global in-memory write queue + read fail-soft shim for qdrant outages.

   Single JVM-wide singleton shared across all projects and agents.
   When the circuit breaker opens, mutating protocol calls enqueue here
   instead of failing; reads return a degraded response. On :closed
   transition, drain! flushes the queue via a single-writer core.async
   pipeline, coalescing each entry id's ops, in the order they were queued,
   into the one mutation that leaves the entry as replaying them all would.

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

(defn- coalesce-key
  "Every op on one entry id shares a key, whatever the op: they fold in the
   order they were queued (`fold-op`). An op with no id (:cleanup-expired!)
   keys on its op."
  [op]
  (if-let [id (:id op)]
    [::entry id]
    [::singleton (:op op)]))

(defn- fold-updates
  "Two queued update-entry! UPDATES maps for one id, U1 then U2, as the one
   map that applies both. A later :content with no :embed-text of its own
   retires the earlier :embed-text, which described the content it replaces:
   run in turn, U2 would have embedded its own :content, not U1's text.
   U1 may also be a whole queued entry: the same merge folds an update into
   the add queued before it."
  [u1 u2]
  (merge (if (and (contains? u2 :content) (not (contains? u2 :embed-text)))
           (dissoc u1 :embed-text)
           u1)
         u2))

(defn- fold-op
  "The one op that stands for PREV then OP, two mutations of one entry id
   (PREV nil when OP is the first). Replaying it leaves the entry as
   replaying both in turn would, last write winning.

   An :add-entry! or :delete-entry! carries its whole outcome, so it
   replaces PREV: an upsert overwrites the whole point (a delete queued
   before it included), a delete removes it. An :update-entry! carries only
   the fields it changes (the store queues the updates alone and merges them
   onto the real entry at replay), so it folds INTO PREV:

     add e,    update u  ->  add e with u merged in (`fold-updates`)
     update v, update u  ->  one update carrying both (`fold-updates`)
     delete,   update u  ->  the delete: update-entry! of an entry that is
                             gone answers nil and writes nothing
     (first)   update u  ->  itself; the replay merges onto the stored entry"
  [prev op]
  (if (and prev (= :update-entry! (:op op)))
    (let [[id updates] (:args op)]
      (case (:op prev)
        :add-entry!    (let [[entry] (:args prev)]
                         ;; update-entry! merges {:id id} last: the id holds.
                         (assoc prev :args [(assoc (fold-updates entry updates) :id id)]))
        :update-entry! (assoc op :args [id (fold-updates (second (:args prev)) updates)])
        :delete-entry! prev
        op))
    op))

(defn coalesce
  "Reduce ops to one mutation per entry id: that id's ops folded in the
   order they were queued (`fold-op`), so replaying the one op left leaves
   the entry as replaying them all in turn would. Ids keep first-seen order.

   The key is the id alone, never (op, id). Keyed by (op, id), an add and an
   update of one id folded apart and replayed in the first-seen order of the
   two keys: add v1, update u, add v2 replayed as add v2 then update u, and
   u's stale fields landed over v2."
  [ops]
  (let [[order folded]
        (reduce (fn [[order folded] op]
                  (let [k (coalesce-key op)]
                    [(if (contains? folded k) order (conj order k))
                     (assoc folded k (fold-op (get folded k) op))]))
                [[] {}]
                ops)]
    (mapv folded order)))

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
      ;; Back AHEAD of anything queued while this pass ran: those ops are
      ;; newer, and a failed op replayed after them would undo them.
      (swap! the-queue #(into (into PersistentQueue/EMPTY failed-ops) %)))
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
