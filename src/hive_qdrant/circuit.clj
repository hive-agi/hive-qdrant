(ns hive-qdrant.circuit
  "Circuit breaker for qdrant protocol operations.

   States: :closed (normal) | :open (fail-fast) | :half-open (probing).

   Mirrors hive-milvus.circuit. Single atom state; listener callbacks
   fire on :closed transitions so queue.drain! can flush on recovery."
  (:require [taoensso.timbre :as log]))

;; =========================================================================
;; State
;; =========================================================================

(def ^:private default-state
  {:state       :closed
   :failures    0
   :opened-at   nil
   :threshold   5
   :cooldown-ms 15000})

(def ^:private breaker-state (atom default-state))
(def ^:private listeners (atom #{}))

;; =========================================================================
;; Config
;; =========================================================================

(defn configure!
  "Update breaker tunables without changing :state. Only non-nil keys merge."
  [{:keys [threshold cooldown-ms]}]
  (swap! breaker-state
         (fn [s]
           (cond-> s
             (some? threshold)   (assoc :threshold threshold)
             (some? cooldown-ms) (assoc :cooldown-ms cooldown-ms))))
  nil)

(defn state [] @breaker-state)
(defn open?      [] (= :open      (:state @breaker-state)))
(defn closed?    [] (= :closed    (:state @breaker-state)))
(defn half-open? [] (= :half-open (:state @breaker-state)))

;; =========================================================================
;; Listeners
;; =========================================================================

(defn subscribe!
  "Register 0-arity fn invoked on :closed transitions. Idempotent."
  [f]
  (swap! listeners conj f)
  f)

(defn unsubscribe! [f]
  (swap! listeners disj f)
  f)

(defn- notify-closed! []
  (doseq [f @listeners]
    (try (f)
         (catch Throwable t
           (log/warn t "circuit listener threw on :closed transition")))))

;; =========================================================================
;; Check / record
;; =========================================================================

(defn check
  "Gate a protocol call. nil when allowed (:closed or :half-open probe).
   Returns fail map when :open and still in cooldown:
     {:success? false :error :circuit-open :retry-after ms :reconnecting? true}"
  []
  (let [now   (System/currentTimeMillis)
        new-s (swap! breaker-state
                     (fn [{:keys [state opened-at cooldown-ms] :as s}]
                       (if (and (= state :open)
                                opened-at
                                (>= (- now opened-at) cooldown-ms))
                         (assoc s :state :half-open)
                         s)))]
    (case (:state new-s)
      :closed    nil
      :half-open nil
      :open      (let [remaining (max 0 (- (:cooldown-ms new-s)
                                           (- now (:opened-at new-s))))]
                   {:success?      false
                    :error         :circuit-open
                    :retry-after   remaining
                    :reconnecting? true}))))

(defn record-failure!
  "Record a connection-failure outcome. :closed -> inc; trip at threshold.
   :half-open -> reopen. :open -> no-op."
  []
  (let [now  (System/currentTimeMillis)
        prev (swap-vals! breaker-state
                         (fn [{:keys [state failures threshold] :as s}]
                           (case state
                             :closed
                             (let [f' (inc (or failures 0))]
                               (if (>= f' (or threshold 5))
                                 (assoc s :state :open :failures f' :opened-at now)
                                 (assoc s :failures f')))

                             :half-open
                             (assoc s :state :open
                                      :opened-at now
                                      :failures  (or threshold 5))

                             :open s)))]
    (let [before (first prev) after (second prev)]
      (when (and (not= :open (:state before)) (= :open (:state after)))
        (log/warn "qdrant circuit OPENED after" (:failures after) "failures; cooldown"
                  (:cooldown-ms after) "ms")))
    nil))

(defn record-success!
  "Record success. :closed -> reset. :half-open|:open -> close + notify."
  []
  (let [[before after]
        (swap-vals! breaker-state
                    (fn [{:keys [state] :as s}]
                      (case state
                        :closed    (assoc s :failures 0)
                        :half-open (assoc s :state :closed :failures 0 :opened-at nil)
                        :open      (assoc s :state :closed :failures 0 :opened-at nil))))]
    (when (and (not= :closed (:state before)) (= :closed (:state after)))
      (log/info "qdrant circuit CLOSED (recovered from" (:state before) ")")
      (notify-closed!))
    nil))

(defn force-reset!
  "Force :closed. Tests + admin recovery. Does NOT notify listeners."
  []
  (reset! breaker-state default-state)
  nil)
