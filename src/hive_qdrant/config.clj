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
     QDRANT_TLS     — enable TLS (bool coercion)    (default: false)
     QDRANT_READ_DEADLINE_MS:   gRPC deadline of a read RPC, ms   (default: 15000)
     QDRANT_SEARCH_DEADLINE_MS: gRPC deadline of a search RPC, ms (default: 60000)

   The two deadlines are read by hive-qdrant.deadline, which bounds every
   read RPC the store sends; writes carry none."
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
  ;; The two deadlines are :string on purpose. An :int field that fails to
  ;; coerce fails the whole resolution, and with it the addon's initialize!:
  ;; a typo in a tuning knob would keep the store from starting. The value is
  ;; handed over as written and hive-qdrant.deadline parses it, falling back
  ;; to the default (and saying so) when it is unusable.
  :read-deadline-ms (env "QDRANT_READ_DEADLINE_MS"
                         :default 15000
                         :type :string
                         :doc "gRPC deadline of a retrieve, scroll or count RPC, in ms.")
  :search-deadline-ms (env "QDRANT_SEARCH_DEADLINE_MS"
                           :default 60000
                           :type :string
                           :doc "gRPC deadline of a search RPC, in ms.")
  :collection-name (literal "hive_qdrant_memory"
                            :doc "Default collection — overridable via addon config."))
