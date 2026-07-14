(ns hive-qdrant.failure
  "Failure taxonomy + classifier for hive-qdrant.

   Classifies Throwables into a closed set of :error/* keywords and
   translates to legacy {:success? false :errors [..] :reconnecting? ..}
   map shape expected by IMemoryStore callers.

   Mirrors hive-milvus.failure in intent but uses plain keywords
   instead of hive-dsl/adt to keep deps minimal."
  (:require [clojure.string :as str]))

(def ^:private transient-markers
  ["UNAVAILABLE"
   "DEADLINE_EXCEEDED"
   "connection"
   "not connected"
   "Keepalive"
   "channel"
   "io exception"])

(defn- transient-message? [^String msg]
  (when msg
    (let [s (str/lower-case (str msg))]
      (boolean (some #(str/includes? s (str/lower-case %)) transient-markers)))))

(defn classify
  "Classify a Throwable into one of :error/transient | :error/fatal |
   :error/reconnect-timeout. Returns a map
   {:variant kw :message str}."
  [^Throwable t]
  (let [msg (or (some-> t .getMessage) (str t))]
    (cond
      (transient-message? msg)
      {:variant :error/transient :message msg}

      :else
      {:variant :error/fatal :message msg})))

(defn transient? [failure]
  (= :error/transient (:variant failure)))

(defn reconnect-timeout
  "Build a :error/reconnect-timeout failure."
  [message]
  {:variant :error/reconnect-timeout :message (or message "reconnect timeout")})

(defn ->legacy-map
  "Translate a failure map to the legacy fail-map shape expected by
   hive-mcp protocol callers:
     {:success? false :errors [msg] :reconnecting? bool}
   :reconnecting? is true for transient + reconnect-timeout; false for fatal."
  [{:keys [variant message] :as _failure}]
  {:success?      false
   :errors        [(str message)]
   :reconnecting? (not= :error/fatal variant)})
