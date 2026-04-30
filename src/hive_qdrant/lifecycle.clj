(ns hive-qdrant.lifecycle
  "IShutdownHook for the Qdrant client. Priority 210 (client band).

   Resolves the Qdrant store disconnect path at runtime via
   `requiring-resolve` so this ns has no compile-time coupling to
   hive-mcp addon orchestration. If no close fn is on the classpath
   (e.g. during isolated unit tests), the hook logs debug and no-ops.

   Registers itself with hive-mcp.system.registry at ns-load time via a
   private `defonce` guard. Hive-mcp's `system/layer1` arranges for this
   ns to be required after the qdrant addon's own `init-as-addon!`
   runs, so the hook is in place by the time the first shutdown signal
   arrives."
  ;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
  ;;
  ;; SPDX-License-Identifier: AGPL-3.0-or-later
  (:require [hive-mcp.protocols.lifecycle :as lifecycle]
            [hive-mcp.system.registry :as reg]
            [taoensso.timbre :as log]))

;; =============================================================================
;; QdrantShutdown — priority 210 (client band)
;; =============================================================================

(defrecord QdrantShutdown []
  lifecycle/IShutdownHook
  (shutdown-priority [_] 210)
  (shutdown-name     [_] "qdrant/close")
  (shutdown!         [_ _ctx]
    (try
      ;; Preferred entry: top-level close! on the store ns (if ever added).
      ;; Fallback: protocol-style disconnect! on a store instance resolved
      ;; via the hive-mcp memory registry — best-effort, never throws.
      (if-let [close-fn (requiring-resolve 'hive-qdrant.store/close!)]
        (do (close-fn)
            (log/info "qdrant shutdown complete"))
        (let [disconnect! (requiring-resolve 'hive-mcp.protocols.memory/disconnect!)
              get-store   (requiring-resolve 'hive-mcp.protocols.memory/get-store)]
          (if (and disconnect! get-store)
            (when-let [store (get-store :carto)]
              (disconnect! store)
              (log/info "qdrant shutdown complete via memory registry"))
            (log/debug "qdrant close fn not on classpath — skipping"))))
      (catch Throwable t
        (log/error t "qdrant close failed")))))

;; =============================================================================
;; Registration
;; =============================================================================

(defn install! []
  (reg/register-shutdown! (->QdrantShutdown)))

;; Auto-register on ns load. Hive-mcp's layer1 requires this ns during
;; server init; operator scripts can also require it directly.
(defonce ^:private -registered?
  (do (install!) true))
