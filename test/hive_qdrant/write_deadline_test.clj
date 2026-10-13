(ns hive-qdrant.write-deadline-test
  "Writes, and the creates connect! sends, carry a gRPC deadline too.

   A store that answered its health check and stalls afterwards parked
   connect! on the collection create, and parked every write: the thread
   never came back, so no failure was recorded, the circuit never opened and
   nothing reached the replay queue. These tests drive the real java client
   against hive-qdrant.stalled-qdrant and watch the wire."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-dsl.result :as r]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.config :as cfg]
            [hive-qdrant.connect :as connect]
            [hive-qdrant.deadline :as deadline]
            [hive-qdrant.queue :as queue]
            [hive-qdrant.stalled-qdrant :as stalled]
            [hive-qdrant.store :as store]
            [hive-spi.memory.ports :as proto]))

(use-fixtures :each
  (fn [t]
    (circuit/force-reset!)
    (queue/clear!)
    (t)
    (circuit/force-reset!)
    (queue/clear!)))

(def ^:private hang-ms 20000)

(defn- timed
  "Run THUNK on its own thread: {:ms elapsed} plus :value, or :threw, or
   {:hung? true} when it had not come back after `hang-ms`."
  [thunk]
  (let [t0 (System/nanoTime)
        f  (future (try {:value (thunk)} (catch Throwable t {:threw t})))
        r  (deref f hang-ms ::hung)]
    (if (= ::hung r)
      (do (future-cancel f) {:hung? true :ms hang-ms})
      (assoc r :ms (quot (- (System/nanoTime) t0) 1000000)))))

(defn- await-cancelled
  "The calls SERVER saw cancelled, once METHOD is among them or 3 s passed."
  [server method]
  (let [until (+ (System/currentTimeMillis) 3000)]
    (while (and (not (some #{method} (stalled/cancelled server)))
                (< (System/currentTimeMillis) until))
      (Thread/sleep 20))
    (stalled/cancelled server)))

;; ---- connect! ---------------------------------------------------------------

(deftest connect-to-a-store-that-stalls-after-its-health-check-fails
  (testing "healthy at the health check, silent after: the probe times out,
            the create it falls back to misses the write deadline, and
            connect! fails instead of parking"
    (with-open [server (stalled/start)]
      (let [s (store/create-store {:vector-size 4})
            {:keys [hung? value ms]}
            (timed #(proto/connect! s (assoc (stalled/config server) :write-deadline-ms 400)))]
        (is (not hung?) "connect! came back")
        (is (< ms (+ connect/exists-probe-timeout-ms 3000)) (str "took " ms " ms"))
        (is (false? (:success? value)) (pr-str value))
        (is (str/includes? (str (first (:errors value))) "collection create") (pr-str value))
        (is (str/includes? (str (first (:errors value))) "DEADLINE_EXCEEDED") (pr-str value))
        (is (false? (proto/connected? s)))
        (is (nil? @(:client-atom s)) "the failed connect kept no client")
        (is (some #{"qdrant.Collections/Create"}
                  (await-cancelled server "qdrant.Collections/Create"))
            "the create was cancelled on the wire, not left in flight"))))
  (testing "the probe says the collection is missing and the create stalls"
    (with-open [server (stalled/start)]
      (let [s (store/create-store {:vector-size 4})
            {:keys [hung? value ms]}
            (with-redefs [connect/collection-exists? (constantly false)]
              (timed #(proto/connect! s (assoc (stalled/config server) :write-deadline-ms 400))))]
        (is (not hung?) "connect! came back")
        (is (< ms 3000) (str "took " ms " ms, the write deadline is 400"))
        (is (false? (:success? value)) (pr-str value))
        (is (false? (proto/connected? s)))))))

(deftest a-stalled-payload-index-create-fails-the-ensure
  (testing "the collection exists, the index create stalls: the ensure throws"
    (with-open [server (stalled/start)]
      (let [client (connect/open! (assoc (stalled/config server) :write-deadline-ms 300))]
        (try
          (let [{:keys [hung? threw ms]}
                (timed #(with-redefs [connect/collection-exists? (constantly true)]
                          (connect/ensure-collection! client {:collection-name "c"
                                                              :vector-size     4})))]
            (is (not hung?))
            (is (< ms 3000) (str "took " ms " ms"))
            (is (= :qdrant/unresponsive (:error (ex-data threw))) (pr-str threw)))
          (finally ((requiring-resolve 'clj-qdrant.client/close!) client)))))))

;; ---- writes -----------------------------------------------------------------

(defn- stalled-store
  "A store wired through the real java client to the stalled SERVER, with
   CONFIG merged over a 4-wide vector config."
  [server config]
  (let [s (store/create-store (merge {:vector-size 4} config))]
    (reset! (:client-atom s) (connect/open! (merge (stalled/config server) config)))
    (reset! (:connected?-atom s) true)
    s))

(deftest a-write-to-a-stalled-store-ends-fails-and-is-queued
  (with-open [server (stalled/start)]
    (let [s (stalled-store server {:write-deadline-ms 400})]
      (try
        (doseq [[label method thunk]
                [["add-entry!" "qdrant.Points/Upsert"
                  #(proto/add-entry! s {:id "w1" :type "note" :content "c"
                                        :embedding [0.1 0.2 0.3 0.4]})]
                 ["delete-entry!" "qdrant.Points/Delete"
                  #(proto/delete-entry! s "w2")]]]
          (testing label
            (circuit/force-reset!)
            (queue/clear!)
            (let [{:keys [hung? value ms]} (timed thunk)]
              (is (not hung?) "the write came back: its thread is free")
              (is (< ms 3000) (str "took " ms " ms, the write deadline is 400"))
              (is (true? (:queued? value)) (pr-str value))
              (is (= 1 (:depth (queue/stats))))
              (is (pos? (:failures (circuit/state))) "the failure reached the circuit")
              (is (some #{method} (await-cancelled server method))))))
        (finally (proto/disconnect! s))))))

(deftest a-metadata-write-ends-within-the-write-deadline
  (with-open [server (stalled/start)]
    (let [s (stalled-store server {:write-deadline-ms 400 :read-deadline-ms 400})]
      (try
        (let [{:keys [hung? ms]} (timed #(proto/update-metadata! s "m1" {:tags ["t"]}))]
          (is (not hung?))
          (is (< ms 3000) (str "took " ms " ms")))
        (finally (proto/disconnect! s))))))

;; ---- config -----------------------------------------------------------------

(deftest the-write-deadline-resolves-like-the-others
  (is (= deadline/default-write-deadline-ms (deadline/write-deadline-ms {})))
  (is (= 700 (deadline/write-deadline-ms {:write-deadline-ms " 700 "})))
  (is (= deadline/default-write-deadline-ms
         (deadline/write-deadline-ms {:write-deadline-ms "soon"})))
  (is (= 900 (deadline/write-deadline-ms
              (deadline/with-deadlines {} {:write-deadline-ms 900}) {})))
  (let [res (cfg/resolve-QdrantConfig {} {:env-fn {"QDRANT_WRITE_DEADLINE_MS" "2500"}})]
    (is (r/ok? res) (pr-str res))
    (is (= 2500 (deadline/write-deadline-ms (:ok res))))))

(deftest the-write-view-answers-no-read
  (with-open [server (stalled/start)]
    (let [raw  (connect/open! (stalled/config server))
          view (deadline/write-client raw {})]
      (try
        (is (thrown? IllegalArgumentException
                     ((requiring-resolve 'clj-qdrant.api/get-points)
                      view :collection "c" :ids [(java.util.UUID/randomUUID)])))
        (is (empty? (stalled/calls server)) "nothing was sent")
        (finally ((requiring-resolve 'clj-qdrant.client/close!) raw))))))
