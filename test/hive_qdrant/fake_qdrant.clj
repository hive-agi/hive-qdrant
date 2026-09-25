(ns hive-qdrant.fake-qdrant
  "An in-memory stand-in for the qdrant java client, so a test can drive the
   store's LIVE branch (the one that upserts points) without a cluster.

   clj-qdrant.api calls its client reflectively (.upsertAsync, .retrieveAsync,
   .searchAsync, .deleteAsync; clj-qdrant.schema .createCollectionAsync and
   .deleteCollectionAsync), so any object answering those methods is a client.
   This one keeps the PointStructs the store upserts: a test reads back what
   would have gone over the wire (payload keys, vector) through `payload` and
   `point-vector`, instead of redefining the api functions.

   Search ranks every stored point by cosine similarity and ignores the
   request's filter: enough for single-scope tests, not a filter oracle.

     (def c (client))
     (reset! (:client-atom store) {:client c})
     (reset! (:connected?-atom store) true)"
  (:import [io.qdrant.client.grpc Points$PointStruct Points$RetrievedPoint
            Points$ScoredPoint Points$SearchPoints Points$VectorOutput
            Points$VectorsOutput]
           [java.util List]
           [java.util.concurrent CompletableFuture]))

(definterface IFakeQdrant
  (upsertAsync [collection points])
  (retrieveAsync [collection ids withPayload withVectors consistency])
  (searchAsync [request])
  (deleteAsync [collection ids-or-filter])
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

(defn- vectors-output
  "A RetrievedPoint's vectors, on the legacy data field, which is where the
   store's retrieved-vector reads them."
  [ps]
  (-> (Points$VectorsOutput/newBuilder)
      (.setVector (-> (Points$VectorOutput/newBuilder)
                      (.addAllData ^Iterable (map float (stored-vector ps)))
                      .build))
      .build))

(defn- retrieved [^Points$PointStruct ps]
  (-> (Points$RetrievedPoint/newBuilder)
      (.setId (.getId ps))
      (.putAllPayload (.getPayloadMap ps))
      (.setVectors ^Points$VectorsOutput (vectors-output ps))
      .build))

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

(deftype FakeQdrant [store]
  IFakeQdrant
  (upsertAsync [_ _collection points]
    (swap! store into (map (fn [^Points$PointStruct ps] [(point-key (.getId ps)) ps])) points)
    (done nil))
  (retrieveAsync [_ _collection ids _with-payload _with-vectors _consistency]
    (done (into [] (keep #(some-> (get @store (point-key %)) retrieved)) ids)))
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
  (createCollectionAsync [_ _request] (done nil))
  (deleteCollectionAsync [_ _collection]
    (reset! store {})
    (done nil)))

(defn client
  "A fresh, empty fake qdrant client."
  []
  (->FakeQdrant (atom {})))

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
