(ns hive-qdrant.connect
  "What the store does to a qdrant when it connects: open the client, then
   make sure the collection and its payload indexes exist.

   Split from hive-qdrant.store, which calls it from connect!.

   The client comes from `hive-qdrant.deadline/open-client`, which tells
   three stores apart:

     answered   the collection is ensured
     unreached  (refused, unroutable, packets dropped) the client is kept and
                nothing is sent: the store is down, and the circuit and the
                replay queue own what goes through that client
     silent     (connected, answers nothing) open! throws, and connect! fails"
  (:require [clj-qdrant.schema :as q-schema]
            [hive-qdrant.deadline :as deadline]
            [taoensso.timbre :as log])
  (:import [java.util.concurrent Future TimeUnit]
[java.util.concurrent TimeoutException]))

(defn open!
  "Open a qdrant client from the store config CFG ({:host :port :api-key
   :tls?}, plus the two deadline keys). The client-map it answers carries
   CFG's read deadlines, so the reads sent through it use them. Throws when
   the server is connected and silent."
  [cfg]
  (deadline/with-deadlines (deadline/open-client cfg) cfg))

(defn- unresponsive
  "The :qdrant/unresponsive failure for STEP, a connect-time RPC the store
   did not answer within its deadline, caused by T."
  [step ^Throwable t]
  (ex-info (str "qdrant answered its health check and then no " step
                " (DEADLINE_EXCEEDED)")
           {:error :qdrant/unresponsive :step step}
           t))

(defn- timed-out?
  "True when T says the server did not answer in time: gRPC's
   DEADLINE_EXCEEDED, or the local budget of a `.get` running out."
  [^Throwable t]
  (or (instance? TimeoutException t) (deadline/deadline-exceeded? t)))

(defn- ensure-payload-indexes!
  "Create payload indexes used by query-entries. Idempotent: soft-fails
   when an index already exists. Required fields: tags (keyword), type
   (keyword), project-id (keyword), content-hash (keyword).

   Each create carries the write deadline (CLIENT-MAP is a write view); one
   the store does not answer in time is no soft failure, and throws."
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
        (when (timed-out? t) (throw (unresponsive "payload index create" t)))
        (log/debug "payload-index-create! soft-failed for" field
                   "(likely already exists):" (ex-message t))))))

(def ^:const exists-probe-timeout-ms 5000)

(defn collection-exists?
  "Boundary: IO. Ask qdrant whether COLLECTION-NAME exists. true/false is
   the server's answer; nil means the probe failed with an error, so the
   caller cannot tell. A probe the server did not answer within
   `exists-probe-timeout-ms` throws :qdrant/unresponsive: the store answered
   its health check and then went silent, and a create sent after it would
   only wait out its own deadline.

   The probe RPC carries that budget as its gRPC deadline, so an unanswered
   probe is cancelled rather than left in flight behind the timed-out `.get`."
  [client-map collection-name]
  (try
    (let [client (:client (deadline/bounded client-map exists-probe-timeout-ms))]
      (boolean (.get ^Future (.collectionExistsAsync client collection-name)
                     exists-probe-timeout-ms TimeUnit/MILLISECONDS)))
    (catch Throwable t
      (when (timed-out? t) (throw (unresponsive "collection exists probe" t)))
      (log/debug "collection-exists? probe failed:" (ex-message t))
      nil)))

(defn ensure-collection!
  "Create the collection if missing + ensure payload indexes.

   Probes `collection-exists?` first so a reconnect against an existing
   collection never sends a create (whose ALREADY_EXISTS the java client
   logs at ERROR with a stack trace). When the probe answers false, or
   fails with an error (nil), it falls back to create-and-swallow: a
   concurrent creator may still win the race, and that 'already exists'
   stays benign.

   Every RPC here is bounded: the probe by `exists-probe-timeout-ms`, the
   creates by the write deadline (`deadline/write-client`). One the store
   does not answer in time throws :qdrant/unresponsive, so connect! fails
   instead of parking on a store that answered its health check and then
   stalled.

   A client opened on a store it could not reach (`:reached? false`) sends
   nothing: every create would fail, or wait out its deadline for a
   connection that is not coming."
  [client-map {:keys [collection-name vector-size distance]
               :or   {distance :cosine}
               :as   cfg}]
  (if (false? (:reached? client-map))
    (log/warn "hive-qdrant: qdrant unreachable at connect;" collection-name
              "is not ensured, writes go to the replay queue")
    (let [writer (deadline/write-client client-map cfg)]
      (when-not (true? (collection-exists? client-map collection-name))
        (try
          (q-schema/collection-create! writer
                                       {:name        collection-name
                                        :vector-size vector-size
                                        :distance    distance})
          (catch Throwable t
            (when (timed-out? t) (throw (unresponsive "collection create" t)))
            (log/debug "collection-create! soft-failed (likely already exists):"
                       (ex-message t))
            nil)))
      (ensure-payload-indexes! writer collection-name))))
