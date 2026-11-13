(ns hive-qdrant.queue-live-order-test
  "A write made while older writes of the same entry wait in the queue must
   not overtake them. Applied directly, it lands first and the older queued
   op, replayed later, overwrites it: the entry ends as the OLDER write left
   it. In order, it queues behind them and the replay leaves the entry as
   applying every call in turn would.

   Over hive-qdrant.fake-qdrant, no live qdrant. The circuit is opened by
   recording failures and closed with force-reset!, as a recovery would."
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
  "A store over a fresh fake client, connected."
  []
  (let [s (store/create-store {:collection-name "live-order" :vector-size 4
                               :embedder (fn [_] [1.0 0.0 0.0 0.0])})
        c (fake/client)]
    (reset! (:client-atom s) {:client c})
    (reset! (:connected?-atom s) true)
    {:store s :client c}))

(defn- open-circuit! []
  (dotimes [_ 10] (circuit/record-failure!))
  (assert (circuit/open?)))

(defn- drain! [s]
  (q/drain! {:dispatch-fn (q/make-store-dispatch s)}))

(deftest a-live-update-after-recovery-queues-behind-the-older-queued-update
  (let [{s :store c :client} (live-store)]
    (proto/add-entry! s {:id "p" :type :note :content "v0" :tags ["t0"]})
    (open-circuit!)
    (is (:queued? (proto/update-entry! s "p" {:content "v1-queued" :tags ["t1"]})))
    (circuit/force-reset!)
    (testing "the live write waits behind the queued one instead of landing first"
      (is (:queued? (proto/update-entry! s "p" {:content "v2-live"})))
      (is (= "v0" (:content (fake/payload c "p")))))
    (drain! s)
    (testing "after the replay the entry is what the two calls, in turn, leave"
      (is (= "v2-live" (:content (fake/payload c "p"))))
      (is (= ["t1"] (vec (:tags (fake/payload c "p"))))))
    (is (zero? (q/size)))))

(deftest a-live-add-or-delete-after-recovery-queues-behind-the-older-queued-op
  (let [{s :store c :client} (live-store)]
    (proto/add-entry! s {:id "p" :type :note :content "v0"})
    (open-circuit!)
    (proto/delete-entry! s "p")
    (circuit/force-reset!)
    (is (:queued? (proto/add-entry! s {:id "p" :type :note :content "re-added"})))
    (drain! s)
    (is (= "re-added" (:content (fake/payload c "p"))))))

(deftest a-write-made-while-a-drain-pass-runs-its-entry-lands-after-it
  (let [{s :store c :client} (live-store)
        dispatch (q/make-store-dispatch s)
        once     (atom false)]
    (proto/add-entry! s {:id "p" :type :note :content "v0"})
    (open-circuit!)
    (proto/update-entry! s "p" {:content "v1-queued"})
    (circuit/force-reset!)
    ;; The live write arrives after the pass took its snapshot, before the
    ;; snapshot's op for the same id has run. It runs on its own thread, as a
    ;; caller would: a plain Thread conveys no binding of the replay.
    (q/drain! {:dispatch-fn (fn [op]
                              (when (compare-and-set! once false true)
                                (doto (Thread. #(proto/update-entry! s "p" {:content "v2-live"}))
                                  (.start)
                                  (.join)))
                              (dispatch op))})
    (is (= "v2-live" (:content (fake/payload c "p"))))
    (is (zero? (q/size)))))

(deftest a-live-write-on-an-entry-with-nothing-queued-goes-straight-through
  (let [{s :store c :client} (live-store)]
    (proto/add-entry! s {:id "p" :type :note :content "v0"})
    (proto/add-entry! s {:id "other" :type :note :content "o0"})
    (open-circuit!)
    (proto/update-entry! s "other" {:content "o1-queued"})
    (circuit/force-reset!)
    (let [r (proto/update-entry! s "p" {:content "v1"})]
      (is (not (:queued? r)))
      (is (= "v1" (:content (fake/payload c "p")))))
    (is (= 1 (q/size)))))

(deftest pending-reads-the-collection-and-the-id
  (q/enqueue! {:op :update-entry! :id "p" :args ["p" {}] :collection "a"})
  (is (q/pending? {:op :add-entry! :id "p" :collection "a"}))
  (is (not (q/pending? {:op :add-entry! :id "p" :collection "b"})))
  (is (not (q/pending? {:op :add-entry! :id "x" :collection "a"}))))
