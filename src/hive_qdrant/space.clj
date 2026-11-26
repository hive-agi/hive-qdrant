(ns hive-qdrant.space
  "Embedding-space provenance of a qdrant collection.

   A dimension match says only that two vectors have the same width. This
   namespace keeps the full space a collection was created in: provider,
   model, exact digest, dimension, metric, normalization, and the query
   template and document serializer revisions. The space is written to the
   collection's own metadata (qdrant >= 1.16 collection metadata) and checked
   whenever an existing collection is opened, so vectors of two spaces never
   share a collection.

   The value layer (stamp, admit, audit) is hive-spi.embeddings.space; this
   namespace only translates between its flat scalar stamp and qdrant's
   JsonWithInt$Value metadata map, and reads the metadata off the wire.

     space->qdrant-metadata   stamp as the Value map a CreateCollection takes
     qdrant-metadata->stamp   the Value map read back as a plain stamp
     distance->metric         qdrant distance keyword -> space metric
     collection-metadata      IO: the stamp of an existing collection
     admit!                   guard: stamped ok, legacy warns, other throws"
  (:require [hive-spi.embeddings.space :as space]
            [taoensso.timbre :as log])
  (:import [io.qdrant.client ValueFactory]
           [io.qdrant.client.grpc Collections$CollectionInfo JsonWithInt$Value]
           [java.util.concurrent Future TimeUnit]))

;; SPDX-License-Identifier: MIT

(def distance->metric
  "The EmbeddingSpace metric a qdrant distance scores with. Manhattan has no
   space metric, so a collection under it cannot be stamped."
  {:cosine :cosine
   :euclid :l2
   :dot    :ip})

(defn space-for-distance
  "SPACE with its :metric set from the qdrant DISTANCE the collection is
   created with, or nil when SPACE is nil or the distance has no metric."
  [space distance]
  (when-let [metric (and space (distance->metric (or distance :cosine)))]
    (space/->space (assoc space :metric metric))))

(defn- ->value
  ^JsonWithInt$Value [v]
  (cond
    (string? v)  (ValueFactory/value ^String v)
    (integer? v) (ValueFactory/value (long v))
    (boolean? v) (ValueFactory/value (boolean v))
    :else        (ValueFactory/value (str v))))

(defn- value->clj [^JsonWithInt$Value v]
  (cond
    (.hasStringValue v)  (.getStringValue v)
    (.hasIntegerValue v) (.getIntegerValue v)
    (.hasBoolValue v)    (.getBoolValue v)
    (.hasDoubleValue v)  (.getDoubleValue v)
    :else                nil))

(defn space->qdrant-metadata
  "The metadata that stamps a new collection with SPACE: the hive-spi stamp,
   each value a JsonWithInt$Value under its key name."
  [space]
  (into {}
        (map (fn [[k v]] [(name k) (->value v)]))
        (space/space->metadata space)))

(defn qdrant-metadata->stamp
  "The plain keyword-keyed stamp of a qdrant metadata map MD (string keys,
   JsonWithInt$Value values). Empty or nil MD answers {}."
  [md]
  (into {}
        (keep (fn [[k v]]
                (when-some [x (value->clj v)] [(keyword k) x])))
        md))

(def ^:const metadata-read-timeout-ms 5000)

(defn collection-metadata
  "Boundary: IO. The stamp of the existing collection COLLECTION-NAME, as a
   plain keyword map ({} when it carries no metadata). CLIENT-MAP must hold a
   client answering getCollectionInfoAsync (a deadline read view, or a fake).
   Throws whatever the RPC throws."
  [client-map collection-name]
  (let [^Collections$CollectionInfo info
        (.get ^Future (.getCollectionInfoAsync (:client client-map) ^String collection-name)
              metadata-read-timeout-ms TimeUnit/MILLISECONDS)]
    (qdrant-metadata->stamp (.. info getConfig getMetadataMap))))

(defonce ^:private legacy-warned (atom #{}))

(defn reset-legacy-warnings!
  "Test helper: forget which legacy collections were already warned about."
  []
  (reset! legacy-warned #{}))

(defn admit!
  "Guard collection COLLECTION-NAME, whose stamp is MD, for vectors of SPACE.

     stamped, same space  => the hive-spi Admission, :verdict :stamped
     no stamp             => :verdict :legacy, warned once per collection
     stamped, other space => throws ex-info :embedding-space-mismatch naming
                             both spaces, the differing keys, the collection
                             and the remedy #{:isolate :re-embed}"
  [collection-name space md]
  (let [{:keys [verdict space-id] :as admission} (space/admit collection-name space md)]
    (when (and (= :legacy verdict) (not (contains? @legacy-warned collection-name)))
      (swap! legacy-warned conj collection-name)
      (log/warn "hive-qdrant: embedding space unrecorded for legacy collection"
                collection-name "- admitting vectors of space" space-id
                "unverified; isolate or re-embed to stamp it"))
    admission))
