(ns hive-qdrant.fake-qdrant
  "An in-memory stand-in for the qdrant java client, so a test can drive the
   store's LIVE branch (the one that upserts points) without a cluster.

   clj-qdrant.api calls its client reflectively (.upsertAsync, .retrieveAsync,
   .searchAsync, .deleteAsync; clj-qdrant.schema .createCollectionAsync and
   .deleteCollectionAsync; hive-qdrant.store .collectionExistsAsync), so any
   object answering those methods is a client.
   This one keeps the PointStructs the store upserts: a test reads back what
   would have gone over the wire (payload keys, vector) through `payload` and
   `point-vector`, instead of redefining the api functions. It also keeps the
   names of the collections created on it, and counts the create calls
   (`create-calls`), so a test can see whether connect sent a create.

   Retrieved points carry their vector where a real qdrant puts it: on the
   VectorOutput `dense` field, with the deprecated `data` field empty (probed
   against qdrant 1.17.1). `(client {:vector-field :data})` answers the way a
   pre-dense server did, to exercise a reader's fallback.

   Search ranks every stored point by cosine similarity and ignores the
   request's filter: enough for single-scope tests, not a filter oracle.

     (def c (client))
     (reset! (:client-atom store) {:client c})
     (reset! (:connected?-atom store) true)"
  (:import [io.qdrant.client.grpc Collections$CreateCollection
            Points$DenseVector Points$PointStruct
            Points$RetrievedPoint Points$ScoredPoint Points$SearchPoints
            Points$VectorOutput Points$VectorsOutput]
           [java.util List]
           [java.util.concurrent CompletableFuture]))

(definterface IFakeQdrant
  (upsertAsync [collection points])
  (retrieveAsync [collection ids withPayload withVectors consistency])
  (searchAsync [request])
  (deleteAsync [collection ids-or-filter])
  (collectionExistsAsync [collection])
  (createCollectionAsync [request])
  (deleteCollectionAsync [collection]))

(defn- done [v] (CompletableFuture/completedFuture v))

(defn- point-key
  "The stored-point key for a qdrant PointId: its uuid string."
  [point-id]
  (.getUuid ^io.qdrant.client.grpc.Common$PointId point-id))

(defn- stored-vector
  "The vector a PointStruct carries. clj-qdrant writes the dense field; the
   deprecated data field is read as a fallback."
  [^Points$PointStruct ps]
  (let [v (.. ps getVectors getVector)]
    (vec (if (.hasDense v) (.. v getDense getDataList) (.getDataList v)))))

(defn vector-output
  "A VectorOutput carrying the floats XS on FIELD: :dense, where qdrant
   1.17 answers a retrieve, or :data, the deprecated field older servers
   filled."
  [field xs]
  (let [floats ^Iterable (map float xs)
        b      (Points$VectorOutput/newBuilder)]
    (.build (case field
              :dense (.setDense b (-> (Points$DenseVector/newBuilder)
                                      (.addAllData floats)
                                      .build))
              :data  (.addAllData b floats)))))

(defn retrieved-point
  "A RetrievedPoint whose vectors output is VECTOR-OUTPUT (nil: no vectors)."
  [^Points$PointStruct ps vector-output]
  (let [b (-> (Points$RetrievedPoint/newBuilder)
              (.setId (.getId ps))
              (.putAllPayload (.getPayloadMap ps)))]
    (when vector-output
      (.setVectors b ^Points$VectorsOutput
                   (-> (Points$VectorsOutput/newBuilder)
                       (.setVector ^Points$VectorOutput vector-output)
                       .build)))
    (.build b)))

(defn- retrieved [vector-field ^Points$PointStruct ps]
  (retrieved-point ps (vector-output vector-field (stored-vector ps))))

(defn- cosine [a b]
  (let [dot (reduce + (map * a b))
        na  (Math/sqrt (reduce + (map * a a)))
        nb  (Math/sqrt (reduce + (map * b b)))]
    (if (or (zero? na) (zero? nb)) 0.0 (/ dot (* na nb)))))

(defn- scored [^Points$PointStruct ps score]
  (-> (Points$ScoredPoint/newBuilder)
      (.setId (.getId ps))
      (.putAllPayload (.getPayloadMap ps))
      (.setScore (float score))
      .build))

(deftype FakeQdrant [store vector-field collections creates]
  IFakeQdrant
  (upsertAsync [_ _collection points]
    (swap! store into (map (fn [^Points$PointStruct ps] [(point-key (.getId ps)) ps])) points)
    (done nil))
  (retrieveAsync [_ _collection ids _with-payload _with-vectors _consistency]
    (done (into [] (keep #(some->> (get @store (point-key %)) (retrieved vector-field))) ids)))
  (searchAsync [_ request]
    (let [^Points$SearchPoints req request
          q (vec (.getVectorList req))]
      (done (->> (vals @store)
                 (map (fn [ps] [ps (cosine q (stored-vector ps))]))
                 (sort-by second >)
                 (take (.getLimit req))
                 (mapv (fn [[ps s]] (scored ps s)))))))
  (deleteAsync [_ _collection ids-or-filter]
    (when (instance? List ids-or-filter)
      (swap! store #(apply dissoc % (map point-key ids-or-filter))))
    (done nil))
  (collectionExistsAsync [_ collection]
    (done (contains? @collections collection)))
  (createCollectionAsync [_ request]
    (swap! creates inc)
    (swap! collections conj (.getCollectionName ^Collections$CreateCollection request))
    (done nil))
  (deleteCollectionAsync [_ collection]
    (swap! collections disj collection)
    (reset! store {})
    (done nil)))

(defn client
  "A fresh, empty fake qdrant client. Retrieved vectors ride the `dense`
   field, as a real server answers; {:vector-field :data} puts them on the
   deprecated `data` field instead. It remembers the collections created on
   it (`collectionExistsAsync` answers from them) and counts every create
   call, see `create-calls`."
  ([] (client {}))
  ([{:keys [vector-field] :or {vector-field :dense}}]
   (->FakeQdrant (atom {}) vector-field (atom #{}) (atom 0))))

(defn create-calls
  "How many times createCollectionAsync reached the fake C, whether or not
   the collection already existed."
  [^FakeQdrant c]
  @(.-creates c))

(defn collections
  "The names of the collections created on the fake C and not deleted."
  [^FakeQdrant c]
  @(.-collections c))

(defn broken-client
  "A client whose every call throws an exception with MESSAGE, the way the
   java client fails. hive-qdrant.failure/classify reads the message: one
   naming UNAVAILABLE is transient, anything else fatal."
  [message]
  (let [boom #(throw (RuntimeException. ^String message))]
    (reify IFakeQdrant
      (upsertAsync [_ _ _] (boom))
      (retrieveAsync [_ _ _ _ _ _] (boom))
      (searchAsync [_ _] (boom))
      (deleteAsync [_ _ _] (boom))
      (collectionExistsAsync [_ _] (boom))
      (createCollectionAsync [_ _] (boom))
      (deleteCollectionAsync [_ _] (boom)))))

(defn flaky-upserts
  "A client over the fake C whose upserts throw an exception with MESSAGE
   while the atom FAILING? holds true; every other call, reads included,
   reaches C. Read stored points back through C itself."
  [^FakeQdrant c failing? message]
  (reify IFakeQdrant
    (upsertAsync [_ collection pts]
      (if @failing?
        (throw (RuntimeException. ^String message))
        (.upsertAsync c collection pts)))
    (retrieveAsync [_ collection ids with-payload with-vectors consistency]
      (.retrieveAsync c collection ids with-payload with-vectors consistency))
    (searchAsync [_ request] (.searchAsync c request))
    (deleteAsync [_ collection ids-or-filter] (.deleteAsync c collection ids-or-filter))
    (collectionExistsAsync [_ collection] (.collectionExistsAsync c collection))
    (createCollectionAsync [_ request] (.createCollectionAsync c request))
    (deleteCollectionAsync [_ collection] (.deleteCollectionAsync c collection))))

(defn- points
  "Every PointStruct upserted into C and still stored."
  [^FakeQdrant c]
  (vals @(.-store c)))

(defn- value->clj [^io.qdrant.client.grpc.JsonWithInt$Value v]
  (cond
    (.hasStringValue v)  (.getStringValue v)
    (.hasIntegerValue v) (.getIntegerValue v)
    (.hasDoubleValue v)  (.getDoubleValue v)
    (.hasBoolValue v)    (.getBoolValue v)
    (.hasListValue v)    (mapv value->clj (.. v getListValue getValuesList))
    :else                nil))

(defn payloads
  "The payload of every stored point as a keyword-keyed map: exactly the
   fields the store wrote to qdrant."
  [c]
  (mapv (fn [^Points$PointStruct ps]
          (into {} (map (fn [[k v]] [(keyword k) (value->clj v)])) (.getPayloadMap ps)))
        (points c)))

(defn payload
  "The stored payload whose :id is ID, or nil."
  [c id]
  (some #(when (= id (:id %)) %) (payloads c)))

(defn point-vector
  "The stored vector of the point whose payload :id is ID, or nil."
  [c id]
  (some (fn [^Points$PointStruct ps]
          (when (= id (some-> (.getPayloadMap ps) (get "id") value->clj))
            (stored-vector ps)))
        (points c)))
