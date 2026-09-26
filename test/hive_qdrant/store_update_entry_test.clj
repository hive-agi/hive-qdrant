(ns hive-qdrant.store-update-entry-test
  "update-entry! merges onto the entry as stored, never onto a read that
   failed.

   The defect: with the circuit open get-entry answers the circuit's failure
   map, {:success? false :error :circuit-open ...}. update-entry! merged it
   with the updates and handed the result to add-entry!, which queued it; the
   replay then overwrote the entry with that map plus the updates, its
   :content and :tags lost. Now an unreadable entry queues the updates alone
   and the replay merges them onto the real entry.

   Driven through hive-qdrant.fake-qdrant, whose points are what would have
   gone over the wire. No live qdrant."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-spi.memory.ports :as proto]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.queue :as q]
            [hive-qdrant.store :as store]))

(use-fixtures :each
  (fn [f]
    (circuit/force-reset!)
    (q/clear!)
    (f)
    (circuit/force-reset!)
    (q/clear!)))

(defn- live-store
  "A store on its LIVE branch over a fresh fake client. Returns [store client]."
  []
  (let [s (store/create-store {:collection-name "test-update-entry" :vector-size 4})
        c (fake/client)]
    (reset! (:client-atom s) {:client c})
    (reset! (:connected?-atom s) true)
    [s c]))

(defn- open-circuit! []
  (circuit/configure! {:threshold 1 :cooldown-ms 60000})
  (circuit/record-failure!))

(defn- replay!
  "Close the circuit and drain the queue into S. Returns the ops replayed,
   without their enqueue timestamps."
  [s]
  (let [ops      (atom [])
        dispatch (q/make-store-dispatch s)]
    (circuit/force-reset!)
    (q/drain! {:dispatch-fn (fn [op] (swap! ops conj (dissoc op :ts)) (dispatch op))})
    @ops))

(def ^:private failure-keys
  "Keys of resilient's failure maps; none may ever reach a payload."
  [:success? :error :errors :retry-after :reconnecting?])

(defn- carries-a-failure? [payload]
  (boolean (some #(contains? payload %) failure-keys)))

;; =============================================================================
;; Circuit open
;; =============================================================================

(deftest an-open-circuit-update-replays-onto-the-real-entry
  (let [[s c] (live-store)]
    (proto/add-entry! s {:id "u1" :type :note :content "keep me" :tags ["t1"]})
    (open-circuit!)
    (is (:queued? (proto/update-entry! s "u1" {:tags ["t2"]}))
        "queued, as every write under an open circuit is")
    (is (= ["t1"] (:tags (fake/payload c "u1"))) "nothing is written while open")
    (testing "the queue holds the updates alone, never a merged failure map"
      (is (= [{:op :update-entry! :id "u1" :args ["u1" {:tags ["t2"]}]}]
             (replay! s))))
    (let [p (fake/payload c "u1")]
      (is (= "keep me" (:content p)) "the replay merged onto the stored entry")
      (is (= ["t2"] (:tags p)))
      (is (not (carries-a-failure? p))))))

(deftest an-update-queued-behind-a-queued-add-keeps-the-add
  (testing "the add and the update both wait out the open circuit"
    (let [[s c] (live-store)]
      (open-circuit!)
      (proto/add-entry! s {:id "u2" :type :note :content "new entry" :tags ["t1"]})
      (proto/update-entry! s "u2" {:tags ["t2"]})
      (replay! s)
      (let [p (fake/payload c "u2")]
        (is (= "new entry" (:content p))
            "before: the update was queued as an add-entry! of the failure map, which replaced the add")
        (is (= ["t2"] (:tags p)))
        (is (not (carries-a-failure? p)))))))

(deftest successive-queued-updates-all-land
  (let [[s c] (live-store)]
    (proto/add-entry! s {:id "u3" :type :note :content "c" :tags ["t1"] :duration "short"})
    (open-circuit!)
    (proto/update-entry! s "u3" {:tags ["t2"]})
    (proto/update-entry! s "u3" {:duration "long"})
    (is (= 1 (count (replay! s))) "the two updates fold into one replay")
    (let [p (fake/payload c "u3")]
      (is (= ["t2"] (:tags p)) "the earlier update is not dropped by coalescing")
      (is (= "long" (:duration p)))
      (is (= "c" (:content p))))))

(deftest a-queued-update-does-not-resurrect-a-queued-delete
  (let [[s c] (live-store)]
    (proto/add-entry! s {:id "u4" :type :note :content "c"})
    (open-circuit!)
    (proto/delete-entry! s "u4")
    (proto/update-entry! s "u4" {:tags ["x"]})
    (replay! s)
    (is (nil? (fake/payload c "u4")) "the update finds no entry and mints none")))

;; =============================================================================
;; Replay order: an id's ops replay as they were queued
;; =============================================================================
;;
;; queue/coalesce once keyed ops by (op, id): an add and an update of one id
;; folded under different keys and replayed in the first-seen order of the
;; keys, not of the ops. Each case below replayed to the wrong entry.

(deftest an-add-queued-after-an-update-is-the-last-write
  (testing "S1: add v1, update, add v2, all under an open circuit"
    (let [[s c] (live-store)]
      (open-circuit!)
      (proto/add-entry! s {:id "x1" :type :note :content "v1" :tags ["a"]})
      (proto/update-entry! s "x1" {:tags ["b"]})
      (proto/add-entry! s {:id "x1" :type :note :content "v2" :tags ["c"]})
      (is (= [:add-entry!] (mapv :op (replay! s))) "the three fold into one write")
      (let [p (fake/payload c "x1")]
        (is (= "v2" (:content p)))
        (is (= ["c"] (:tags p)) "before: the update replayed after v2 and left [b]")))))

(deftest a-queued-update-between-two-queued-adds-leaves-the-last-add
  (testing "S2: update-entry! calls only, the upsert failing transiently"
    (let [c       (fake/client)
          failing (atom false)
          s       (store/create-store {:collection-name "test-update-entry" :vector-size 4})]
      (reset! (:client-atom s) {:client (fake/flaky-upserts c failing "UNAVAILABLE: io exception")})
      (reset! (:connected?-atom s) true)
      (proto/add-entry! s {:id "x2" :type :note :content "c" :tags ["t0"]})
      (circuit/configure! {:threshold 1 :cooldown-ms 60000})
      (reset! failing true)
      (testing "the read works and the upsert fails: the merged entry is queued as an add"
        (is (:queued? (proto/update-entry! s "x2" {:tags ["t1"]}))))
      (testing "that failure opened the circuit: the updates alone are queued"
        (is (circuit/open?))
        (is (:queued? (proto/update-entry! s "x2" {:tags ["t2"]}))))
      (circuit/force-reset!)
      (testing "closed again, the upsert still failing: another add is queued"
        (is (:queued? (proto/update-entry! s "x2" {:tags ["t3"]}))))
      (reset! failing false)
      (is (= [:add-entry!] (mapv :op (replay! s))))
      (let [p (fake/payload c "x2")]
        (is (= ["t3"] (:tags p)) "before: the queued update replayed last and left [t2]")
        (is (= "c" (:content p)))))))

(deftest an-update-queued-after-a-delete-and-re-add-lands-on-the-re-add
  (testing "S3: update, delete, add, update, all under an open circuit"
    (let [[s c] (live-store)]
      (proto/add-entry! s {:id "x3" :type :note :content "old" :tags ["t0"]})
      (open-circuit!)
      (proto/update-entry! s "x3" {:tags ["u1"]})
      (proto/delete-entry! s "x3")
      (proto/add-entry! s {:id "x3" :type :note :content "new" :tags ["e2"]})
      (proto/update-entry! s "x3" {:duration "long"})
      (is (= [:add-entry!] (mapv :op (replay! s))))
      (let [p (fake/payload c "x3")]
        (is (= "new" (:content p)))
        (is (= ["e2"] (:tags p)))
        (is (= "long" (:duration p))
            "before: both updates folded, replayed first, then the delete and the add wiped them")))))

;; =============================================================================
;; A read that throws
;; =============================================================================

(deftest a-transient-read-failure-queues-the-updates-alone
  (let [[s c] (live-store)]
    (proto/add-entry! s {:id "u5" :type :note :content "keep me" :tags ["t1"]})
    (reset! (:client-atom s) {:client (fake/broken-client "UNAVAILABLE: io exception")})
    (is (:queued? (proto/update-entry! s "u5" {:tags ["t2"]})))
    (reset! (:client-atom s) {:client c})
    (replay! s)
    (let [p (fake/payload c "u5")]
      (is (= "keep me" (:content p)))
      (is (= ["t2"] (:tags p)))
      (is (not (carries-a-failure? p))))))

(deftest a-fatal-read-failure-fails-loudly
  (let [[s c] (live-store)]
    (proto/add-entry! s {:id "u6" :type :note :content "keep me" :tags ["t1"]})
    (reset! (:client-atom s) {:client (fake/broken-client "INVALID_ARGUMENT: bad request")})
    (let [r (proto/update-entry! s "u6" {:tags ["t2"]})]
      (is (false? (:success? r)))
      (is (false? (:reconnecting? r))))
    (is (zero? (q/size)) "a failure that will not heal is not queued")
    (reset! (:client-atom s) {:client c})
    (is (= ["t1"] (:tags (fake/payload c "u6"))) "nothing is written")))

;; =============================================================================
;; Unknown id
;; =============================================================================

(deftest an-unknown-id-answers-nil-and-mints-nothing
  (testing "live"
    (let [[s c] (live-store)]
      (is (nil? (proto/update-entry! s "ghost" {:tags ["x"]})))
      (is (nil? (fake/payload c "ghost")))
      (is (zero? (q/size)))))
  (testing "fallback"
    (let [s (store/create-store {:vector-size 4})]
      (is (nil? (proto/update-entry! s "ghost" {:tags ["x"]})))
      (is (nil? (proto/get-entry s "ghost"))))))
