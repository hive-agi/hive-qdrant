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
  (:import [java.util.concurrent Future TimeUnit]))

(defn open!
  "Open a qdrant client from the store config CFG ({:host :port :api-key
   :tls?}, plus the two deadline keys). The client-map it answers carries
   CFG's read deadlines, so the reads sent through it use them. Throws when
   the server is connected and silent."
  [cfg]
  (deadline/with-deadlines (deadline/open-client cfg) cfg))

(defn- ensure-payload-indexes!
  "Create payload indexes used by query-entries. Idempotent: soft-fails
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

(def ^:const exists-probe-timeout-ms 5000)

(defn collection-exists?
  "Boundary: IO. Ask qdrant whether COLLECTION-NAME exists. true/false is
   the server's answer; nil means the probe itself failed (threw, or did not
   answer within `exists-probe-timeout-ms`), so the caller cannot tell.

   The probe RPC carries that budget as its gRPC deadline, so an unanswered
   probe is cancelled rather than left in flight behind the timed-out `.get`."
  [client-map collection-name]
  (try
    (let [client (:client (deadline/bounded client-map exists-probe-timeout-ms))]
      (boolean (.get ^Future (.collectionExistsAsync client collection-name)
                     exists-probe-timeout-ms TimeUnit/MILLISECONDS)))
    (catch Throwable t
      (log/debug "collection-exists? probe failed:" (ex-message t))
      nil)))

(defn ensure-collection!
  "Create the collection if missing + ensure payload indexes.

   Probes `collection-exists?` first so a reconnect against an existing
   collection never sends a create (whose ALREADY_EXISTS the java client
   logs at ERROR with a stack trace). When the probe answers false, or
   fails (nil), it falls back to create-and-swallow: a concurrent creator
   may still win the race, and that 'already exists' stays benign.

   A client opened on a store it could not reach (`:reached? false`) sends
   nothing: every create would fail, or wait with no deadline for a
   connection that is not coming."
  [client-map {:keys [collection-name vector-size distance]
               :or   {distance :cosine}}]
  (if (false? (:reached? client-map))
    (log/warn "hive-qdrant: qdrant unreachable at connect;" collection-name
              "is not ensured, writes go to the replay queue")
    (do
      (when-not (true? (collection-exists? client-map collection-name))
        (try
          (q-schema/collection-create! client-map
                                       {:name        collection-name
                                        :vector-size vector-size
                                        :distance    distance})
          (catch Throwable t
            (log/debug "collection-create! soft-failed (likely already exists):"
                       (ex-message t))
            nil)))
      (ensure-payload-indexes! client-map collection-name))))
