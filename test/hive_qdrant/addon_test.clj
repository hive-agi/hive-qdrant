(ns hive-qdrant.addon-test
  "Trifecta for QdrantAddon: golden + property + mutation.

   Live qdrant connection is swapped out via with-redefs on
   hive-qdrant.store/create-store → always-connect stub, so registry
   registration paths are exercised without a cluster."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-mcp.addons.protocol :as addon-proto]
            [hive-mcp.protocols.memory :as mem-proto]
            [hive-qdrant.addon :as addon]
            [hive-qdrant.store :as store]))

(use-fixtures :each
  (fn [f]
    (let [snapshot (mem-proto/registered-stores)]
      (mem-proto/unregister-store! :carto)
      (mem-proto/unregister-store! :kanban)
      (try
        (f)
        (finally
          (mem-proto/unregister-store! :carto)
          (mem-proto/unregister-store! :kanban)
          (doseq [[k store] snapshot]
            (when (#{:carto :kanban} k)
              (mem-proto/register-store! k store))))))))

(defrecord StubStore [state]
  mem-proto/IMemoryStore
  (connect!       [_ _] {:success? true :errors []})
  (disconnect!    [_]   nil)
  (connected?     [_]   true)
  (health-check   [_]   {:healthy? true :status :ok :backend "qdrant"})
  (add-entry!     [_ e] {:success? true :id (:id e)})
  (get-entry      [_ _] nil)
  (update-entry!  [_ _ _] {:success? true})
  (delete-entry!  [_ _] {:success? true})
  (query-entries  [_ _] [])
  (search-similar [_ _ _] {:success? true :results []})
  (supports-semantic-search? [_] true)
  (cleanup-expired! [_] {:success? true})
  (entries-expiring-soon [_ _ _] [])
  (find-duplicate [_ _ _ _] nil)
  (store-status   [_] {:backend "qdrant" :connected? true})
  (reset-store!   [_] {:success? true}))

(defn- stub-store [] (->StubStore (atom {})))

;; ---- Golden ---------------------------------------------------------------

(deftest golden-addon-identity
  (let [a (addon/create-addon)]
    (is (= "hive.qdrant" (addon-proto/addon-id a)))
    (is (= :native       (addon-proto/addon-type a)))
    (is (contains? (addon-proto/capabilities a) :vector-search))
    (is (contains? (addon-proto/capabilities a) :cartography))))

(deftest golden-tools-and-schema-empty
  (let [a (addon/create-addon)]
    (is (= [] (addon-proto/tools a)))
    (is (= {} (addon-proto/schema-extensions a)))
    (is (= #{} (addon-proto/excluded-tools a)))))

(deftest golden-health-before-init
  (let [a (addon/create-addon)
        h (addon-proto/health a)]
    (is (= :down (:status h)))
    (is (= "not initialized" (-> h :details :reason)))))

(deftest golden-initialize-registers-under-carto
  (with-redefs [store/create-store (fn [_] (stub-store))]
    (let [a (addon/create-addon)
          r (addon-proto/initialize! a {:host "x" :port 6334})]
      (is (:success? r))
      (is (= :carto (-> r :metadata :slot)))
      (is (some? (mem-proto/get-store :carto))))))

;; ---- Property -------------------------------------------------------------

(deftest property-initialize-then-shutdown-cycle
  (with-redefs [store/create-store (fn [_] (stub-store))]
    (let [a (addon/create-addon)]
      (addon-proto/initialize! a {})
      (is (some? (mem-proto/get-store :carto)))
      (addon-proto/shutdown! a)
      (is (thrown? clojure.lang.ExceptionInfo (mem-proto/get-store :carto))))))

(deftest property-repeated-initialize-idempotent
  (with-redefs [store/create-store (fn [_] (stub-store))]
    (let [a (addon/create-addon)]
      (addon-proto/initialize! a {})
      (addon-proto/initialize! a {})
      (is (some? (mem-proto/get-store :carto))))))

;; ---- Mutation -------------------------------------------------------------

(deftest mutation-port-string-coercion
  (with-redefs [store/create-store (fn [cfg]
                                     (is (integer? (:port cfg)))
                                     (stub-store))]
    (let [a (addon/create-addon)]
      (addon-proto/initialize! a {:port "6334"}))))

(deftest mutation-empty-host-defaults
  (with-redefs [store/create-store (fn [cfg]
                                     (is (= "localhost" (:host cfg)))
                                     (stub-store))]
    (let [a (addon/create-addon)]
      (addon-proto/initialize! a {:host ""}))))

;; ---- Slot + addon-id parameterization ------------------------------------

(deftest golden-create-addon-with-config-overrides-slot-and-id
  (let [a (addon/create-addon {:registry-key :kanban
                               :addon/id     "hive.qdrant.kanban"})]
    (is (= "hive.qdrant.kanban" (addon-proto/addon-id a)))))

(deftest golden-initialize-registers-under-kanban-when-config-routes
  (with-redefs [store/create-store (fn [_] (stub-store))]
    (let [a (addon/create-addon {:registry-key :kanban
                                 :addon/id     "hive.qdrant.kanban"})
          r (addon-proto/initialize! a {:registry-key :kanban
                                        :host "x" :port 6334})]
      (is (:success? r))
      (is (= :kanban (-> r :metadata :slot)))
      (is (some? (mem-proto/get-store :kanban)))
      (is (thrown? clojure.lang.ExceptionInfo (mem-proto/get-store :carto))))))

(deftest property-two-addons-coexist-on-distinct-slots
  (with-redefs [store/create-store (fn [_] (stub-store))]
    (let [carto  (addon/create-addon)
          kanban (addon/create-addon {:registry-key :kanban
                                      :addon/id     "hive.qdrant.kanban"})]
      (addon-proto/initialize! carto  {})
      (addon-proto/initialize! kanban {:registry-key :kanban})
      (is (some? (mem-proto/get-store :carto)))
      (is (some? (mem-proto/get-store :kanban)))
      (is (not= (mem-proto/get-store :carto)
                (mem-proto/get-store :kanban)))
      (addon-proto/shutdown! carto)
      (is (some? (mem-proto/get-store :kanban)) ":kanban survives :carto shutdown")
      (addon-proto/shutdown! kanban))))

(deftest property-shutdown-uses-actual-init-slot
  ;; If create-addon defaulted to :carto but initialize! routed to :kanban,
  ;; shutdown! must unregister :kanban (the slot actually used), not :carto.
  (with-redefs [store/create-store (fn [_] (stub-store))]
    (let [a (addon/create-addon)] ;; default :carto
      (addon-proto/initialize! a {:registry-key :kanban})
      (is (some? (mem-proto/get-store :kanban)))
      (addon-proto/shutdown! a)
      (is (thrown? clojure.lang.ExceptionInfo (mem-proto/get-store :kanban))))))

(deftest mutation-connect-failure-propagates
  (with-redefs [store/create-store
                (fn [_]
                  (reify mem-proto/IMemoryStore
                    (connect!       [_ _] {:success? false :errors ["boom"]})
                    (disconnect!    [_]   nil)
                    (connected?     [_]   false)
                    (health-check   [_]   {:healthy? false})
                    (add-entry!     [_ _] nil)
                    (get-entry      [_ _] nil)
                    (update-entry!  [_ _ _] nil)
                    (delete-entry!  [_ _] nil)
                    (query-entries  [_ _] nil)
                    (search-similar [_ _ _] nil)
                    (supports-semantic-search? [_] false)
                    (cleanup-expired! [_] nil)
                    (entries-expiring-soon [_ _ _] nil)
                    (find-duplicate [_ _ _ _] nil)
                    (store-status   [_] {})
                    (reset-store!   [_] nil)))]
    (let [a (addon/create-addon)
          r (addon-proto/initialize! a {})]
      (is (false? (:success? r)))
      (is (seq (:errors r))))))
