(ns hive-qdrant.addon
  "IAddon implementation for Qdrant vector database backend.

   Registers QdrantMemoryStore in the multi-store registry of
   hive-mcp.protocols.memory. The slot is parameterizable so that more
   than one QdrantAddon instance can coexist (carto + kanban + …) — each
   backed by its own collection. The addon-id is also parameterizable so
   the addon-registry sees them as distinct.

   Slot resolution (highest wins):
     1. `:registry-key` in the initialize! config
     2. `:registry-key` in the create-addon config (creation-time default)
     3. `:carto`                             (backward-compat default)

   addon-id resolution (highest wins):
     1. `:addon/id` injected into config by manifest/prepare-config
     2. `:addon/id` (or `:addon-id`) in the create-addon config
     3. \"hive.qdrant\"                       (backward-compat default)

   Connection config is resolved via hive-di defconfig
   (see `hive-qdrant.config/QdrantConfig`). Env vars QDRANT_HOST,
   QDRANT_PORT, QDRANT_API_KEY, QDRANT_TLS drive the connection;
   explicit values from the addon manifest / init-from-manifest! win as
   overrides.

   Usage (from hive-mcp addon discovery or manual):
     (require '[hive-qdrant.addon :as qdrant-addon])
     (require '[hive-mcp.addons.core :as addons])

     ;; Manifest-driven (the normal path): two manifests under
     ;; META-INF/hive-addons/ — one with :registry-key :carto, one with
     ;; :registry-key :kanban — register two distinct addon-ids.

     ;; Manual:
     (addons/register-addon! (qdrant-addon/create-addon))
     (addons/init-addon! \"hive.qdrant\"
       {:collection-name \"carto_snippets\"})"
  (:require [hive-dsl.result :as r]
            [hive-mcp.addons.protocol :as addon-proto]
            [hive-mcp.protocols.memory :as mem-proto]
            [hive-qdrant.config :as cfg]
            [hive-qdrant.store :as store]
            [taoensso.timbre :as log]))

(def ^:const default-registry-key
  "Default multi-store registry slot when manifest doesn't override."
  :carto)

(def ^:const default-addon-id
  "Default addon-id when manifest doesn't supply one."
  "hive.qdrant")

(defn- resolve-slot
  "Slot precedence: initialize-config :registry-key > creation-time
   slot-atom > :carto default."
  [config slot-atom]
  (or (:registry-key config) @slot-atom default-registry-key))

(defn- resolve-addon-id
  "addon-id precedence: config :addon/id (manifest-injected) > config
   :addon-id (manual) > slot-atom-default."
  [config default-id]
  (or (:addon/id config) (:addon-id config) default-id))

(defrecord QdrantAddon [store-atom slot-atom addon-id-atom]
  addon-proto/IAddon

  (addon-id [_this] @addon-id-atom)

  (addon-type [_this] :native)

  (capabilities [_this]
    #{:vector-search :health-reporting :cartography})

  (initialize! [_this config]
    (try
      (let [;; Hive-di handles env resolution, blank->nil, and type coercion.
            ;; Manifest-supplied config is passed as overrides — explicit
            ;; values win over env, blank strings fall through to env/default.
            cfg-result (cfg/resolve-QdrantConfig (or config {}))
            slot       (resolve-slot config slot-atom)]
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
                (reset! slot-atom slot)
                (mem-proto/register-store! slot store)
                (log/info "QdrantAddon initialized — registered under"
                          slot
                          {:host       (:host resolved)
                           :port       (:port resolved)
                           :tls?       (:tls? resolved)
                           :collection (:collection-name resolved)
                           :addon-id   @addon-id-atom})
                {:success? true :errors [] :metadata {:backend "qdrant"
                                                       :slot    slot}})
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
      (mem-proto/unregister-store! @slot-atom)
      (reset! store-atom nil)
      (log/info "QdrantAddon shut down" {:slot @slot-atom :addon-id @addon-id-atom}))
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
   initialize!. Accepts an optional config map for manifest compatibility.

   Two manifest-supplied keys influence creation-time defaults:
   - `:registry-key`   — registry slot to occupy (default :carto). Lets a
     second manifest register under :kanban.
   - `:addon/id`       — addon-id, defaults to \"hive.qdrant\". Required to
     differ when registering more than one QdrantAddon instance."
  ([]
   (create-addon {}))
  ([config]
   (->QdrantAddon (atom nil)
                  (atom (or (:registry-key config) default-registry-key))
                  (atom (resolve-addon-id config default-addon-id)))))
