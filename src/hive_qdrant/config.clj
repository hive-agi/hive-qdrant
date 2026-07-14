(ns hive-qdrant.config
  "Typed config resolution for the hive-qdrant addon.

   Single source of truth for env-var wiring: host, port, api-key, tls.
   Everything else (collection-name, vector-size, distance) stays as
   internal literals — only the connection surface is operator-tunable.

   Usage:

     (require '[hive-qdrant.config :as cfg])

     ;; Defaults + env
     (cfg/resolve-QdrantConfig)
     ;; => {:ok {:host \"localhost\" :port 6334 :api-key nil :tls? false
     ;;          :collection-name \"hive_qdrant_memory\"}}

     ;; With overrides (manifest-supplied config map wins over env)
     (cfg/resolve-QdrantConfig {:host \"qdrant.svc\" :port \"6334\"})

     ;; With mock env (tests)
     (cfg/resolve-QdrantConfig {} {:env-fn {\"QDRANT_HOST\" \"stub\"}})

   Env contract:
     QDRANT_HOST    — gRPC/HTTP host                (default: localhost)
     QDRANT_PORT    — gRPC port                     (default: 6334)
     QDRANT_API_KEY — bearer token, optional        (default: nil)
     QDRANT_TLS     — enable TLS (bool coercion)    (default: false)"
  (:require [hive-di.core :refer [defconfig env literal]]))

(defconfig QdrantConfig
  :host            (env "QDRANT_HOST"
                        :default "localhost"
                        :type :string
                        :doc "Qdrant host (gRPC or HTTP).")
  :port            (env "QDRANT_PORT"
                        :default 6334
                        :type :int
                        :doc "Qdrant gRPC port. HTTP default is 6333.")
  :api-key         (env "QDRANT_API_KEY"
                        :default nil
                        :type :string
                        :required false
                        :doc "Bearer token for Qdrant Cloud / secured clusters.")
  :tls?            (env "QDRANT_TLS"
                        :default false
                        :type :bool
                        :doc "Enable TLS on the client connection.")
  :collection-name (literal "hive_qdrant_memory"
                            :doc "Default collection — overridable via addon config."))
