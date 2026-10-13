(ns hive-qdrant.deadline
  "Per-call gRPC deadlines for the store's READ RPCs.

   A caller-side timeout (a future deref'd with a budget, as hive-carto's
   vectordb facade does) frees the caller and nothing else: the RPC stays in
   flight and its thread stays parked until the store answers or the
   connection drops. A dead store then costs one parked thread per read.

   The java client takes a `Duration` on every read RPC and turns it into
   `withDeadlineAfter` on the stub, so the call itself ends: gRPC cancels it,
   the future fails with DEADLINE_EXCEEDED, and the blocked `.get` returns.
   hive-qdrant.failure classifies that status as transient.

   clj-qdrant calls its client reflectively and passes no Duration, so the
   deadline is added here, without a change to clj-qdrant: `read-client` and
   `search-client` answer a client-map whose :client is a VIEW of the real
   QdrantClient. The view answers the read methods clj-qdrant and the store
   call, each forwarded to the java client's Duration overload. It answers no
   write method, so a write sent through a read view fails at the call
   instead of silently gaining a deadline.

   Deadlines, in milliseconds, come from the store config:

     :read-deadline-ms    retrieve, scroll, count   (default 15000)
     :search-deadline-ms  search                    (default 60000)

   hive-qdrant.config seeds both from QDRANT_READ_DEADLINE_MS and
   QDRANT_SEARCH_DEADLINE_MS. A missing, non-numeric or non-positive value,
   or one over an hour, falls back to the default: a deadline cannot be
   switched off by accident.

   Writes are left alone. They go through the circuit and the replay queue,
   and a write cancelled mid-flight may or may not have landed.

   A client that is not a QdrantClient (the fakes under test/) is handed back
   unchanged: there is no RPC to bound.

   `open-client` covers the one read the java client sends by itself: the
   health check its builder runs, which has no deadline either."
  (:require [clj-qdrant.client :as q-client]
            [clojure.string :as str])
  (:import [io.grpc ManagedChannelBuilder Status Status$Code]
           [io.qdrant.client QdrantClient QdrantGrpcClient
            WithPayloadSelectorFactory WithVectorsSelectorFactory]
           [io.qdrant.client.grpc Common$Filter
            Points$ReadConsistency Points$ScrollPoints Points$SearchPoints]
           [java.time Duration]
           [java.util List]))

(def ^:const default-read-deadline-ms
  "Deadline of a retrieve, scroll or count RPC. A healthy qdrant answers
   these in 3-17 ms; 15 s is the bound hive-carto's facade already uses."
  15000)

(def ^:const default-search-deadline-ms
  "Deadline of a search RPC, the slowest read on a large collection."
  60000)

(def ^:const max-deadline-ms
  "The longest deadline a config may ask for: one hour. Anything longer is
   a typo or a way of asking for no deadline, and gets the default."
  3600000)

(defn- usable-ms
  "X as a deadline in milliseconds, or nil when it is not a usable one. A
   usable one is an integer, or a string of digits (the two shapes a config
   value arrives in), from 1 to `max-deadline-ms`."
  [x]
  (let [n (cond
            (integer? x) x
            (string? x)  (parse-long (str/trim x)))]
    (when (and n (<= 1 n max-deadline-ms)) (long n))))

(defn read-deadline-ms
  "The read deadline CONFIG asks for, in ms; the default when it asks for
   none or for an unusable one."
  [config]
  (or (usable-ms (:read-deadline-ms config)) default-read-deadline-ms))

(defn search-deadline-ms
  "The search deadline CONFIG asks for, in ms; the default when it asks for
   none or for an unusable one."
  [config]
  (or (usable-ms (:search-deadline-ms config)) default-search-deadline-ms))

(definterface IReadClient
  (retrieveAsync [collection ids withPayload withVectors consistency])
  (scrollAsync [request])
  (searchAsync [request])
  (countAsync [collection filter exact])
  (collectionExistsAsync [collection]))

(defn- bounded-view
  "A read-only view of QC whose every call carries the deadline D."
  [^QdrantClient qc ^Duration d]
  (reify IReadClient
    (retrieveAsync [_ collection ids with-payload with-vectors consistency]
      ;; the java client has no Duration overload of the boolean arity
      ;; clj-qdrant calls, so the booleans become the selectors that arity
      ;; builds from them
      (.retrieveAsync qc ^String collection ^List ids
                      (WithPayloadSelectorFactory/enable (boolean with-payload))
                      (WithVectorsSelectorFactory/enable (boolean with-vectors))
                      ^Points$ReadConsistency consistency d))
    (scrollAsync [_ request]
      (.scrollAsync qc ^Points$ScrollPoints request d))
    (searchAsync [_ request]
      (.searchAsync qc ^Points$SearchPoints request d))
    (countAsync [_ collection filter exact]
      (.countAsync qc ^String collection ^Common$Filter filter ^Boolean exact d))
    (collectionExistsAsync [_ collection]
      (.collectionExistsAsync qc ^String collection d))))

(defn bounded
  "CLIENT-MAP with its :client replaced by a read view under a deadline of
   MS milliseconds. nil stays nil; a client that is not a QdrantClient is
   handed back unchanged."
  [client-map ms]
  (let [c (:client client-map)]
    (if (instance? QdrantClient c)
      (assoc client-map :client (bounded-view c (Duration/ofMillis (long ms))))
      client-map)))

(defn read-client
  "CLIENT-MAP for a retrieve, scroll or count: every RPC sent through it
   ends within CONFIG's read deadline."
  [client-map config]
  (bounded client-map (read-deadline-ms config)))

(defn search-client
  "CLIENT-MAP for a search: the RPC ends within CONFIG's search deadline."
  [client-map config]
  (bounded client-map (search-deadline-ms config)))

(def ^:const default-connect-deadline-ms
  "How long a freshly opened client waits for the server's first answer."
  5000)

(defn- deadline-exceeded?
  "True when T, or a cause of T, is gRPC's DEADLINE_EXCEEDED."
  [^Throwable t]
  (boolean (some #(= Status$Code/DEADLINE_EXCEEDED (.getCode (Status/fromThrowable %)))
                 (take-while some? (iterate #(.getCause ^Throwable %) t)))))

(defn- silent?
  "True when the server behind QC answers no health check within MS: the
   connection is up and nothing comes back. A server that answers, and one
   that fails at once (refused, unreachable), are both not silent."
  [^QdrantClient qc ms]
  (try
    (.get (.healthCheckAsync qc (Duration/ofMillis (long ms))))
    false
    (catch Throwable t
      (deadline-exceeded? t))))

(defn open-client
  "Open a qdrant client for CFG ({:host :port :api-key :tls?}) and answer the
   client-map clj-qdrant's api takes. Throws when the server accepts the
   connection and then answers nothing within MS (default
   `default-connect-deadline-ms`); the client is closed first.

   clj-qdrant.client/make is not used: the java client's builder sends a
   blocking HealthCheck with NO deadline (its version-compatibility check),
   so against a stalled server `make` itself never returns. The client is
   built here with that check off, and the health check is sent under a
   deadline. A server that is down fails that check at once, is not silent,
   and gets its client-map as before: the circuit and the queue own that case."
  ([cfg] (open-client cfg default-connect-deadline-ms))
  ([{:keys [host port api-key tls?]} ms]
   (let [host    (or host "localhost")
         port    (int (or port 6334))
         chan-b  (-> (ManagedChannelBuilder/forAddress ^String host port)
                     (.maxInboundMessageSize (int q-client/default-max-inbound-message-size)))
         channel (.build (if tls? (.useTransportSecurity chan-b) (.usePlaintext chan-b)))
         b       (QdrantGrpcClient/newBuilder channel false false)
         grpc    (.build (if api-key (.withApiKey b ^String api-key) b))
         client  (QdrantClient. grpc)]
     (when (silent? client ms)
       (.close client)
       (.shutdownNow channel)
       (throw (ex-info (str "qdrant at " host ":" port " accepted the connection and"
                            " answered no health check within " ms " ms"
                            " (DEADLINE_EXCEEDED)")
                       {:error :qdrant/unresponsive :host host :port port :deadline-ms ms})))
     {:client client :grpc-client grpc})))
