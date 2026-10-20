(ns hive-qdrant.deadline-connect-test
  "What connect does with a store it cannot reach, and where the deadlines of
   a connected store come from.

   A store that is UNREACHED (the channel never got READY: packets dropped,
   a peer that takes the TCP connection and speaks no HTTP/2) is down, not
   silent. Its health check ends in DEADLINE_EXCEEDED like a silent store's
   does, and the two must not be confused: a down store keeps its client, so
   the circuit and the replay queue own its writes. Dropping the client would
   move the store to its in-memory fallback, where a write is acknowledged
   and never replayed.

   The unreached peer here is a listening socket that never accepts: the
   kernel completes the TCP handshake and nothing answers the HTTP/2 preface.

   Also: an unusable deadline in the environment never fails config
   resolution, and is reported once."
  (:require [clj-qdrant.client :as q-client]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-dsl.result :as r]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.config :as cfg]
            [hive-qdrant.deadline :as deadline]
            [hive-qdrant.queue :as queue]
            [hive-qdrant.stalled-qdrant :as stalled]
            [hive-qdrant.store :as store]
            [hive-spi.memory.ports :as proto]
            [taoensso.timbre :as log])
  (:import [java.net InetAddress ServerSocket]))

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

(defn- mute-peer
  "A loopback socket that listens and never accepts. Close it when done."
  ^ServerSocket []
  (ServerSocket. 0 50 (InetAddress/getByName "127.0.0.1")))

(defn- peer-config [^ServerSocket peer]
  {:host "127.0.0.1" :port (.getLocalPort peer)})

;; ---- Unreached is down, not silent ----------------------------------------

(deftest opening-a-client-on-an-unreached-store-hands-the-client-over
  (with-open [peer (mute-peer)]
    (let [{:keys [hung? threw value ms]} (timed #(deadline/open-client (peer-config peer) 400))]
      (is (not hung?))
      (is (nil? threw) "DEADLINE_EXCEEDED on a channel that never got READY is not a silent store")
      (is (some? (:client value)))
      (is (false? (:reached? value)))
      (is (< ms 3000) (str "took " ms " ms"))
      (some-> value q-client/close!))))

(deftest a-reached-store-says-so
  (with-open [server (stalled/start)]
    (let [client (deadline/open-client (stalled/config server))]
      (try (is (true? (:reached? client)))
           (finally (q-client/close! client))))))

(deftest connect-to-an-unreached-store-keeps-the-client-and-queues-writes
  (let [peer (mute-peer)
        s    (store/create-store {:vector-size 4})]
    (try
      (let [{:keys [hung? value]} (timed #(proto/connect! s (peer-config peer)))]
        (is (not hung?) "connect! came back")
        (is (true? (:success? value)) (pr-str value))
        (is (some? @(:client-atom s)) "the client is handed to the circuit and the queue")
        (is (true? (proto/connected? s))))
      ;; the peer goes away: from here every RPC fails at once, UNAVAILABLE
      (.close peer)
      (testing "a write is queued for replay, not acknowledged into the fallback"
        (let [{:keys [hung?]} (timed #(proto/add-entry! s {:id "x1" :type "note" :content "c"}))]
          (is (not hung?))
          (is (= 1 (:depth (queue/stats))) (pr-str (queue/stats)))
          (is (empty? (:entries @(:fallback-atom s))))))
      (testing "a read answers a failure, not a missing entry"
        (circuit/force-reset!)
        (let [{:keys [value]} (timed #(proto/get-entry s "x1"))]
          (is (false? (:success? value)) (pr-str value))))
      (finally
        (.close peer)
        (proto/disconnect! s)))))

;; ---- Deadlines given at connect -------------------------------------------

(deftest deadlines-passed-to-connect-are-the-ones-reads-use
  (testing "connect! merges its cfg over the store's: the deadlines ride on the
            client it opens, so a read does not fall back to the defaults"
    (with-open [peer (mute-peer)]
      (let [s (store/create-store {})]
        (try
          (proto/connect! s (assoc (peer-config peer)
                                   :read-deadline-ms 300 :search-deadline-ms 900))
          (let [c @(:client-atom s)]
            (is (= 300 (deadline/read-deadline-ms c (:config s))))
            (is (= 900 (deadline/search-deadline-ms c (:config s)))))
          (finally (proto/disconnect! s))))))
  (testing "on the wire: a read through such a client ends at that deadline"
    (with-open [server (stalled/start)]
      (let [raw (deadline/with-deadlines (deadline/open-client (stalled/config server))
                  {:read-deadline-ms 300})
            ids [(java.util.UUID/randomUUID)]]
        (try
          (let [{:keys [hung? threw ms]}
                (timed #((requiring-resolve 'clj-qdrant.api/get-points)
                         (deadline/read-client raw {}) :collection "c" :ids ids))]
            (is (not hung?))
            (is (str/includes? (str threw) "DEADLINE_EXCEEDED"))
            (is (< ms 2500) (str "took " ms " ms, the default is 15000")))
          (finally (q-client/close! raw)))))))

;; ---- Config: an unusable deadline never takes the store down ---------------

(defn- warnings
  "Every WARN line THUNK logs."
  [thunk]
  (let [seen (atom [])]
    (log/with-merged-config
      {:min-level :debug
       :appenders {:capture {:enabled? true
                             :fn (fn [d] (when (= :warn (:level d))
                                           (swap! seen conj (force (:msg_ d)))))}}}
      (thunk))
    @seen))

(deftest an-unusable-env-deadline-resolves-and-falls-back
  (doseq [[raw want] [["abc" 15000] ["15s" 15000] ["15000.5" 15000]
                      ["0" 15000] ["-5" 15000] ["99999999999" 15000]
                      [" 3000 " 3000] ["3000" 3000]]]
    (testing (pr-str raw)
      (let [res (cfg/resolve-QdrantConfig {} {:env-fn {"QDRANT_READ_DEADLINE_MS" raw}})]
        (is (r/ok? res) (str "a tuning knob must not fail resolution: " (pr-str res)))
        (is (= want (deadline/read-deadline-ms (:ok res))))
        (is (= 60000 (deadline/search-deadline-ms (:ok res))))))))

(deftest an-unusable-deadline-is-reported-once
  (let [config {:read-deadline-ms "never-seen-before-abc"}
        lines  (warnings #(dotimes [_ 3] (deadline/read-deadline-ms config)))]
    (is (= 1 (count lines)) (pr-str lines))
    (is (str/includes? (first lines) "never-seen-before-abc"))
    (is (str/includes? (first lines) "15000")))
  (testing "a usable or absent value says nothing"
    (is (empty? (warnings #(do (deadline/read-deadline-ms {:read-deadline-ms 2000})
                               (deadline/read-deadline-ms {})
                               (deadline/search-deadline-ms nil)))))))
