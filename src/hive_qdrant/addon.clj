(ns hive-qdrant.addon
  "IAddon implementation for Qdrant vector database backend.

   Registers QdrantMemoryStore under the :carto slot of the multi-store
   registry in hive-mcp.protocols.memory. This intentionally does NOT
   take the :default slot — cartography needs its own dedicated store,
   independent of the main memory backend (chroma / milvus).

   Connection config is resolved via hive-di defconfig
   (see `hive-qdrant.config/QdrantConfig`). Env vars QDRANT_HOST,
   QDRANT_PORT, QDRANT_API_KEY, QDRANT_TLS drive the connection;
   explicit values from the addon manifest / init-from-manifest! win as
   overrides.

   Usage (from hive-mcp addon discovery or manual):
     (require '[hive-qdrant.addon :as qdrant-addon])
     (require '[hive-mcp.addons.core :as addons])

     (addons/register-addon! (qdrant-addon/create-addon))
     (addons/init-addon! \"hive.qdrant\"
       {:collection-name \"carto_snippets\"})"
  (:require [hive-dsl.result :as r]
            [hive-mcp.addons.protocol :as addon-proto]
            [hive-mcp.protocols.memory :as mem-proto]
            [hive-qdrant.config :as cfg]
            [hive-qdrant.store :as store]
            [taoensso.timbre :as log]))

(def ^:const registry-key
  "Multi-store registry slot this addon occupies."
  :carto)

(defrecord QdrantAddon [store-atom]
  addon-proto/IAddon

  (addon-id [_this] "hive.qdrant")

  (addon-type [_this] :native)

  (capabilities [_this]
    #{:vector-search :health-reporting :cartography})

  (initialize! [_this config]
    (try
      (let [;; Hive-di handles env resolution, blank->nil, and type coercion.
            ;; Manifest-supplied config is passed as overrides — explicit
            ;; values win over env, blank strings fall through to env/default.
            cfg-result (cfg/resolve-QdrantConfig (or config {}))]
        (if (r/err? cfg-result)
          {:success? false
           :errors   [(str "QdrantConfig resolution failed: "
                           (pr-str (:errors cfg-result)))]
           :metadata {:backend "qdrant"}}
          (let [resolved (merge (:ok cfg-result)
                                ;; Carry through store-only knobs that the
                                ;; operator may override without being part
                                ;; of the typed QdrantConfig surface.
                                (select-keys config [:vector-size :distance]))
                store    (store/create-store
                          (select-keys resolved [:host :port :collection-name
                                                 :api-key :tls? :vector-size :distance]))
                result   (mem-proto/connect! store resolved)]
            (if (:success? result)
              (do
                (reset! store-atom store)
                (mem-proto/register-store! registry-key store)
                (log/info "QdrantAddon initialized — registered under"
                          registry-key
                          {:host       (:host resolved)
                           :port       (:port resolved)
                           :tls?       (:tls? resolved)
                           :collection (:collection-name resolved)})
                {:success? true :errors [] :metadata {:backend "qdrant"
                                                       :slot    registry-key}})
              {:success? false
               :errors   (:errors result)
               :metadata {:backend "qdrant"}}))))
      (catch Exception e
        {:success? false
         :errors   [(.getMessage e)]
         :metadata {:backend "qdrant"}})))

  (shutdown! [_this]
    (when-let [store @store-atom]
      (try (mem-proto/disconnect! store) (catch Throwable _ nil))
      (mem-proto/unregister-store! registry-key)
      (reset! store-atom nil)
      (log/info "QdrantAddon shut down"))
    nil)

  (tools [_this] [])

  (schema-extensions [_this] {})

  (health [_this]
    (if-let [store @store-atom]
      (let [h (mem-proto/health-check store)]
        {:status  (if (:healthy? h) :ok :down)
         :details h})
      {:status :down
       :details {:reason "not initialized"}}))

  (excluded-tools [_this] #{}))

(defn create-addon
  "Create a QdrantAddon instance. Actual configuration applied at
   initialize!. Accepts an optional config map for manifest compatibility."
  ([]
   (->QdrantAddon (atom nil)))
  ([_config]
   (->QdrantAddon (atom nil))))
