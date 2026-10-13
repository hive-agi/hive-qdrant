(ns hive-qdrant.deadline-test
  "Every READ RPC the store sends carries a real gRPC deadline.

   The proof needs a store that is connected and silent, which no fake client
   can be: the deadline lives in the java client's stub, below anything a
   fake replaces. So these tests put the REAL java client in front of
   hive-qdrant.stalled-qdrant, a gRPC server that accepts every call and
   answers none, and watch the wire.

   Golden:   the deadlines a config resolves to, defaults included.
   Property: any config value resolves to a usable deadline; a read ends near
             its own deadline, a search near the search deadline.
   Mutation: the same RPC through the undeadlined client is still parked
             after the deadline, which is the bug; clj-qdrant's own `make`
             never returns against a silent server; and the read view
             answers no write."
  (:require [clj-qdrant.api :as q-api]
            [clj-qdrant.client :as q-client]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-dsl.result :as r]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.config :as cfg]
            [hive-qdrant.deadline :as deadline]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.stalled-qdrant :as stalled]
            [hive-qdrant.store :as store]
            [hive-spi.memory.ports :as proto]
            [hive-qdrant.connect :as connect])
  (:import [java.util.concurrent Future TimeUnit TimeoutException]))

(use-fixtures :each (fn [t] (circuit/force-reset!) (t) (circuit/force-reset!)))

;; A deadline short enough to keep the suite quick and long enough that the
;; first call's connection setup fits inside it.
(def ^:private deadline-ms 400)

;; How long a call may take before the test calls it parked. Far above any
;; deadline used here, far below forever.
(def ^:private hang-ms 12000)

(defn- timed
  "Run THUNK on its own thread. Answers {:ms elapsed} plus :value, or :threw
   when it raised, or {:hung? true} when it had not come back after
   `hang-ms`: an unbounded call fails the test and does not hang the suite."
  [thunk]
  (let [t0 (System/nanoTime)
        f  (future (try {:value (thunk)} (catch Throwable t {:threw t})))
        r  (deref f hang-ms ::hung)]
    (if (= ::hung r)
      (do (future-cancel f) {:hung? true :ms hang-ms})
      (assoc r :ms (quot (- (System/nanoTime) t0) 1000000)))))

(defmacro ^:private with-stalled-store
  "Bind SERVER to a qdrant that was healthy at connect and answers nothing
   since, and STORE to a store wired to it through the real java client, with
   CONFIG merged over a 4-wide vector config."
  [[server store config] & body]
  `(with-open [~server (stalled/start)]
     (let [client# (deadline/open-client (stalled/config ~server))
           ~store  (store/create-store (merge {:vector-size 4} ~config))]
       (reset! (:client-atom ~store) client#)
       (reset! (:connected?-atom ~store) true)
       (try ~@body (finally (q-client/close! client#))))))

(def ^:private scan-ids
  (some-> (ns-resolve 'hive-spi.memory.ports 'scan-ids) deref))

(defn- store-reads
  "Every read the store can send to qdrant: [label rpc-method thunk kind].
   KIND names the deadline the call site must use: :read, :search, or :probe
   for the exists probe, which keeps its own fixed budget."
  [s]
  (cond-> [["get-entry" "qdrant.Points/Get"
            #(proto/get-entry s "an-id") :read]
           ["get-entries" "qdrant.Points/Get"
            #(proto/get-entries s ["a" "b"]) :read]
           ["query-entries" "qdrant.Points/Scroll"
            #(proto/query-entries s {:type "note" :limit 5}) :read]
           ["find-duplicate" "qdrant.Points/Scroll"
            #(proto/find-duplicate s "note" "a-hash" {}) :read]
           ["search-similar" "qdrant.Points/Search"
            #(proto/search-similar s "q" {:embedding [0.1 0.2 0.3 0.4]}) :search]
           ["update-metadata! (its read)" "qdrant.Points/Get"
            #(proto/update-metadata! s "an-id" {:tags ["t"]}) :read]
           ["collection-exists? probe" "qdrant.Collections/CollectionExists"
            #(connect/collection-exists? @(:client-atom s) "a-collection") :probe]]
    scan-ids (conj ["scan-ids" "qdrant.Points/Count" #(scan-ids s {}) :read])))

;; ---- Golden ---------------------------------------------------------------

(deftest golden-deadlines-resolve
  (testing "no config: 15 s reads, 60 s search"
    (is (= 15000 (deadline/read-deadline-ms {})))
    (is (= 60000 (deadline/search-deadline-ms {})))
    (is (= 15000 (deadline/read-deadline-ms nil))))
  (testing "a config value wins, as a number or as the string an env var is"
    (is (= 2500 (deadline/read-deadline-ms {:read-deadline-ms 2500})))
    (is (= 2500 (deadline/read-deadline-ms {:read-deadline-ms " 2500 "})))
    (is (= 90000 (deadline/search-deadline-ms {:search-deadline-ms 90000}))))
  (testing "the two deadlines are independent"
    (is (= 60000 (deadline/search-deadline-ms {:read-deadline-ms 1})))
    (is (= 15000 (deadline/read-deadline-ms {:search-deadline-ms 1}))))
  (testing "an unusable value cannot switch the deadline off"
    (doseq [bad [0 -1 "" "soon" 1.5 :never (inc deadline/max-deadline-ms)]]
      (is (= 15000 (deadline/read-deadline-ms {:read-deadline-ms bad})) (pr-str bad))
      (is (= 60000 (deadline/search-deadline-ms {:search-deadline-ms bad})) (pr-str bad)))))

(deftest golden-config-seeds-the-deadlines
  (testing "defaults"
    (let [m (:ok (cfg/resolve-QdrantConfig {} {:env-fn (constantly nil)}))]
      (is (= 15000 (deadline/read-deadline-ms m)))
      (is (= 60000 (deadline/search-deadline-ms m)))))
  (testing "env: the value arrives as written and is parsed by the deadline ns"
    (let [env {"QDRANT_READ_DEADLINE_MS" "3000" "QDRANT_SEARCH_DEADLINE_MS" "45000"}
          res (cfg/resolve-QdrantConfig {} {:env-fn #(get env %)})]
      (is (r/ok? res))
      (is (= 3000 (deadline/read-deadline-ms (:ok res))))
      (is (= 45000 (deadline/search-deadline-ms (:ok res))))))
  (testing "an override wins over env"
    (let [res (cfg/resolve-QdrantConfig {:read-deadline-ms 700}
                                        {:env-fn {"QDRANT_READ_DEADLINE_MS" "3000"}})]
      (is (= 700 (deadline/read-deadline-ms (:ok res)))))))

(deftest a-fake-client-is-handed-back-unchanged
  (let [cm {:client (fake/client)}]
    (is (identical? cm (deadline/read-client cm {})))
    (is (identical? cm (deadline/search-client cm {})))
    (is (nil? (deadline/read-client nil {})))))

;; ---- The wire: reads ------------------------------------------------------

(deftest every-store-read-ends-within-its-deadline
  ;; The two deadlines are far apart, so the time a call took says which one
  ;; its call site used: a read sent through the search client would end at
  ;; 2000 ms, a search sent through the read client at 400.
  (with-stalled-store [server s {:read-deadline-ms   deadline-ms
                                 :search-deadline-ms 2000}]
    (doseq [[label method thunk kind] (store-reads s)]
      (testing label
        ;; a tripped circuit would answer without sending anything
        (circuit/force-reset!)
        (let [before (count (stalled/calls server))
              {:keys [hung? ms]} (timed thunk)]
          (is (not hung?) "the call came back: its thread is free")
          (case kind
            :read   (is (< ms 1500) (str "took " ms " ms, the read deadline is " deadline-ms))
            :search (is (<= 1800 ms 4500) (str "took " ms " ms, the search deadline is 2000"))
            :probe  (is (< ms (+ connect/exists-probe-timeout-ms 3000)) (str "took " ms " ms")))
          (is (some #{method} (drop before (stalled/calls server)))
              "the RPC really reached the stalled server"))))))

(deftest a-missed-deadline-is-a-transient-failure-not-an-answer
  (with-stalled-store [_server s {:read-deadline-ms deadline-ms}]
    (let [{:keys [value]} (timed #(proto/get-entry s "an-id"))]
      (is (false? (:success? value)))
      (is (true? (:reconnecting? value)) "DEADLINE_EXCEEDED classifies transient")
      (is (str/includes? (str (first (:errors value))) "DEADLINE_EXCEEDED")))
    (testing "find-duplicate keeps its never-throws contract"
      (circuit/force-reset!)
      (let [{:keys [value threw hung?]} (timed #(proto/find-duplicate s "note" "h" {}))]
        (is (not hung?))
        (is (nil? threw))
        (is (nil? value))))))

(deftest the-deadline-cancels-the-rpc-on-the-wire
  (with-stalled-store [server s {:read-deadline-ms deadline-ms}]
    (timed #(proto/get-entry s "an-id"))
    (let [until (+ (System/currentTimeMillis) 3000)]
      (while (and (empty? (stalled/cancelled server))
                  (< (System/currentTimeMillis) until))
        (Thread/sleep 20)))
    (is (= ["qdrant.Points/Get"] (stalled/cancelled server))
        "the server saw the call end: nothing is left in flight")))

(deftest reads-and-search-each-keep-their-own-deadline
  (with-stalled-store [_server s {:read-deadline-ms 300 :search-deadline-ms 1500}]
    (let [read   (timed #(proto/get-entry s "an-id"))
          _      (circuit/force-reset!)
          search (timed #(proto/search-similar s "q" {:embedding [0.1 0.2 0.3 0.4]}))]
      (is (< (:ms read) 1200) (str "read took " (:ms read) " ms"))
      (is (<= 1300 (:ms search) 4000) (str "search took " (:ms search) " ms")))))

;; ---- The wire: connect ----------------------------------------------------

(deftest opening-a-client-on-a-silent-server-ends-within-the-deadline
  (with-open [server (stalled/start {:health? false})]
    (let [{:keys [hung? threw ms]} (timed #(deadline/open-client (stalled/config server) deadline-ms))]
      (is (not hung?))
      (is (< ms 3000) (str "took " ms " ms"))
      (is (= :qdrant/unresponsive (:error (ex-data threw))))
      (is (str/includes? (ex-message threw) "DEADLINE_EXCEEDED")))))

(deftest connect-to-a-silent-store-fails-instead-of-hanging
  (with-open [server (stalled/start {:health? false})]
    (let [s (store/create-store {})
          {:keys [hung? value ms]} (timed #(proto/connect! s (stalled/config server)))]
      (is (not hung?) "connect! came back")
      (is (< ms (+ deadline/default-connect-deadline-ms 3000)) (str "took " ms " ms"))
      (is (false? (:success? value)))
      (is (str/includes? (str (first (:errors value))) "DEADLINE_EXCEEDED"))
      (is (false? (proto/connected? s)))
      (is (empty? (stalled/calls server)) "no create was sent to a silent store"))))

(deftest a-store-that-is-down-is-not-silent
  (testing "a refused connection fails the health check at once: the client
            is handed over as before, and the circuit and queue own the rest"
    (let [port (with-open [server (stalled/start)] (stalled/port server))
          {:keys [hung? threw value ms]}
          (timed #(deadline/open-client {:host "127.0.0.1" :port port}))]
      (is (not hung?))
      (is (nil? threw))
      (is (some? (:client value)))
      (is (< ms deadline/default-connect-deadline-ms) (str "took " ms " ms"))
      (q-client/close! value))))

;; ---- Property -------------------------------------------------------------

(defspec any-config-value-resolves-to-a-usable-deadline 300
  (prop/for-all [v (gen/one-of [gen/any-printable gen/large-integer
                                (gen/fmap str gen/large-integer)])]
    (let [rd (deadline/read-deadline-ms {:read-deadline-ms v})
          sd (deadline/search-deadline-ms {:search-deadline-ms v})]
      (and (integer? rd) (<= 1 rd deadline/max-deadline-ms)
           (integer? sd) (<= 1 sd deadline/max-deadline-ms)))))

(deftest a-read-ends-near-its-deadline-whatever-the-deadline
  (doseq [ms [250 600 1100]]
    (testing (str ms " ms")
      (circuit/force-reset!)
      (with-stalled-store [_server s {:read-deadline-ms ms}]
        (let [{:keys [hung?] took :ms} (timed #(proto/query-entries s {:limit 3}))]
          (is (not hung?))
          (is (<= (- ms 100) took (+ ms 2000))
              (str "a " ms " ms deadline ended the call after " took " ms")))))))

;; ---- Mutation -------------------------------------------------------------

(defn- parked?
  "True when the future F has not come back after MS."
  [^Future f ms]
  (try (.get f ms TimeUnit/MILLISECONDS) false
       (catch TimeoutException _ true)))

(deftest mutant-without-the-deadline-the-call-stays-parked
  (testing "the same retrieve through the undeadlined client, which is what
            the store sent before, is still in flight long after the deadline"
    (with-open [server (stalled/start)]
      (let [raw (deadline/open-client (stalled/config server))
            ids [(java.util.UUID/randomUUID)]]
        (try
          (let [call (future (q-api/get-points raw :collection "c" :ids ids))]
            (is (parked? call (* 3 deadline-ms)) "no deadline: the thread is still blocked")
            (is (empty? (stalled/cancelled server)) "and the RPC is still open")
            (future-cancel call))
          (testing "the read view ends that very call"
            (let [{:keys [hung? threw]}
                  (timed #(q-api/get-points (deadline/bounded raw deadline-ms)
                                            :collection "c" :ids ids))]
              (is (not hung?))
              (is (str/includes? (str threw) "DEADLINE_EXCEEDED"))))
          (finally (q-client/close! raw)))))))

(deftest mutant-clj-qdrant-make-never-returns-on-a-silent-server
  (testing "the java client's builder sends a HealthCheck with no deadline,
            which is why the store opens its client through deadline/open-client"
    (with-open [server (stalled/start {:health? false})]
      (let [call (future (q-client/make (stalled/config server)))]
        (is (parked? call (* 3 deadline-ms)))
        ;; closing the server (with-open) is what frees this thread
        ))))

(deftest mutant-a-write-through-the-read-view-is-refused
  (testing "the view answers no write method, so a write can never pick up a
            read deadline by being handed the wrong client"
    (with-open [server (stalled/start)]
      (let [raw  (deadline/open-client (stalled/config server))
            view (deadline/read-client raw {:read-deadline-ms deadline-ms})]
        (try
          (is (thrown? IllegalArgumentException
                       (q-api/upsert-points view :collection "c"
                                            :points [{:id     (java.util.UUID/randomUUID)
                                                      :vector [0.0 0.0 0.0 0.0]}])))
          (is (thrown? IllegalArgumentException
                       (q-api/delete-points view :collection "c"
                                            :ids [(java.util.UUID/randomUUID)])))
          (is (empty? (stalled/calls server)) "nothing was sent")
          (finally (q-client/close! raw)))))))
