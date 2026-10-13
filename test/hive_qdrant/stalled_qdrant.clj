(ns hive-qdrant.stalled-qdrant
  "A qdrant that accepts every RPC and answers none: a real gRPC server on a
   loopback port whose calls are opened, recorded and never closed. It is what
   a dead-but-connected store looks like from the client: the TCP connection
   and the HTTP/2 stream are up, and no response ever comes.

   Unlike hive-qdrant.fake-qdrant this sits behind the REAL java client, so a
   test drives the store's true transport path and sees what only the wire
   can show: whether the client ends the call or parks on it.

   By default the server still answers the HealthCheck, the way a store that
   was healthy at connect and stalled afterwards did; `{:health? false}`
   stalls that too, which is a store dead from the first byte.

     (with-open [s (stalled/start)]
       (let [c (deadline/open-client (stalled/config s))]
         ...
         (stalled/calls s)       ;; => [\"qdrant.Points/Get\" ...]
         (stalled/cancelled s))) ;; => the calls the client gave up on"
  (:import [io.grpc HandlerRegistry Metadata MethodDescriptor
            MethodDescriptor$Marshaller MethodDescriptor$MethodType Server
            ServerBuilder ServerCall ServerCall$Listener ServerCallHandler
            ServerMethodDefinition Status]
           [io.qdrant.client.grpc QdrantOuterClass$HealthCheckReply]
           [java.io ByteArrayInputStream Closeable InputStream]))

(def health-method
  "The full name of the RPC the store probes liveness with at connect."
  "qdrant.Qdrant/HealthCheck")

(def ^:private bytes-marshaller
  (reify MethodDescriptor$Marshaller
    (stream [_ v] (ByteArrayInputStream. ^bytes v))
    (parse [_ in] (.readAllBytes ^InputStream in))))

(def ^:private health-reply
  (.toByteArray (-> (QdrantOuterClass$HealthCheckReply/newBuilder)
                    (.setTitle "stalled-qdrant")
                    (.setVersion "1.19.0")
                    .build)))

(defn- answer-health!
  "Answer the unary CALL with a HealthCheckReply and close it."
  [^ServerCall call]
  (.sendHeaders call (Metadata.))
  (.sendMessage call health-reply)
  (.close call Status/OK (Metadata.)))

(defn- method-definition
  "A unary method definition for METHOD-NAME. Its handler records the call
   in CALLS and its cancellation in CANCELLED, and never answers, except the
   HealthCheck when HEALTH? is true."
  [^String method-name calls cancelled health?]
  (ServerMethodDefinition/create
   (-> (MethodDescriptor/newBuilder bytes-marshaller bytes-marshaller)
       (.setType MethodDescriptor$MethodType/UNARY)
       (.setFullMethodName method-name)
       .build)
   (reify ServerCallHandler
     (startCall [_ call _headers]
       (swap! calls conj method-name)
       (.request call 1)
       (proxy [ServerCall$Listener] []
         (onHalfClose []
           (when (and health? (= health-method method-name))
             (answer-health! call)))
         (onCancel [] (swap! cancelled conj method-name)))))))

(defrecord StalledQdrant [^Server server calls cancelled]
  Closeable
  (close [_] (.shutdownNow server)))

(defn start
  "Start a stalled qdrant on a free loopback port. Close it when done.
   {:health? false} stalls the HealthCheck as well."
  ([] (start {}))
  ([{:keys [health?] :or {health? true}}]
   (let [calls     (atom [])
         cancelled (atom [])
         registry  (proxy [HandlerRegistry] []
                     (lookupMethod [method-name _authority]
                       (method-definition method-name calls cancelled health?)))
         server    (-> (ServerBuilder/forPort 0)
                       (.fallbackHandlerRegistry registry)
                       .build
                       .start)]
     (->StalledQdrant server calls cancelled))))

(defn port
  "The loopback port the stalled qdrant S listens on."
  [s]
  (.getPort ^Server (:server s)))

(defn config
  "The connection config of the stalled qdrant S, as the store takes it."
  [s]
  {:host "127.0.0.1" :port (port s)})

(defn calls
  "The full method name of every RPC that reached S, in arrival order, the
   health checks left out."
  [s]
  (vec (remove #{health-method} @(:calls s))))

(defn cancelled
  "The full method name of every RPC the client cancelled on S, which is
   what a client deadline does to a call the server never answered."
  [s]
  @(:cancelled s))
