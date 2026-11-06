(ns hive-qdrant.outbox-test
  "The replay queue's on-disk outbox: a queued write outlives the process.

   The queue is driven through its port (hive-qdrant.outbox/IOutbox): the
   file outbox over a temp file for the restart cases, an in-memory stub
   for the failing-disk case."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.test.check.generators :as gen]
            [hive-test.trifecta :refer [deftrifecta]]
            [hive-qdrant.outbox :as outbox]
            [hive-qdrant.queue :as q])
  (:import [java.io File]))

(defn- temp-path []
  (let [f (File/createTempFile "hive-qdrant-outbox" ".edn")]
    (.delete f)
    (.deleteOnExit f)
    (.getPath f)))

(use-fixtures :each
  (fn [f]
    (q/detach-outbox!)
    (q/clear!)
    (try (f)
         (finally
           (q/detach-outbox!)
           (q/clear!)))))

(defn- attach-fresh!
  "Attach a file outbox at PATH as a process starting cold would: nothing
   queued in RAM, everything on disk."
  [path]
  (q/detach-outbox!)
  (q/clear!)
  (q/attach-outbox! (outbox/file-outbox path) path))

(defn- ids [] (mapv :id (:ops (outbox/load-ops (outbox/file-outbox (:path (:outbox (q/stats)))))))) 

;; ---- decode-lines trifecta --------------------------------------------------

(defn run-decode-lines
  "Adapter: case-map -> decode-lines over its :lines."
  [{:keys [lines]}]
  (outbox/decode-lines lines))

(deftrifecta decode-lines-contract
  hive-qdrant.outbox-test/run-decode-lines
  {:golden-path "test/golden/qdrant/outbox/decode-lines.edn"
   :cases       {:empty    {:lines []}
                 :one-op   {:lines ["{:op :delete-entry! :id \"a\" :args [\"a\"]}"]}
                 :blank    {:lines ["" "   "]}
                 :torn     {:lines ["{:op :delete-entry! :id \"a\" :args [\"a\"]}"
                                    "{:op :add-entry! :id \"b\" :args [{:id"]}
                 :not-an-op {:lines ["[1 2 3]" "{:id \"x\"}" "42"]}}
   :gen         (gen/let [n gen/nat]
                  {:lines (mapv #(outbox/encode-op {:op :delete-entry! :id (str %) :args [(str %)]})
                                (range n))})
   :pred        (fn [{:keys [ops corrupt]}]
                  (and (vector? ops) (zero? corrupt)))
   :num-tests   50
   :mutations   [["no-skip" (fn [{:keys [lines]}] {:ops [] :corrupt (count lines)})]
                 ["count-blank" (fn [{:keys [lines]}]
                                  (assoc (outbox/decode-lines lines) :corrupt (count lines)))]]})

;; ---- durable-op --------------------------------------------------------------

(deftest the-disk-record-never-holds-embed-text
  (let [op {:op :add-entry! :id "k" :args [{:id "k" :content "sealed" :embed-text "plain"}]}]
    (is (= {:op :add-entry! :id "k" :args [{:id "k" :content "sealed"}]}
           (outbox/durable-op op)))
    (is (not (str/includes? (outbox/encode-op op) "plain")))))

(deftest an-update-keeps-its-id-arg
  (is (= ["k" {:content "x"}]
         (:args (outbox/durable-op {:op :update-entry! :args ["k" {:content "x" :embed-text "y"}]})))))

;; ---- restart -----------------------------------------------------------------

(deftest a-queued-write-survives-a-restart
  (let [path (temp-path)]
    (attach-fresh! path)
    (let [r (q/enqueue! {:op :add-entry! :id "a" :args [{:id "a"}] :collection "c"})]
      (is (:spilled? r) "recorded on disk before the answer")
      (is (false? (:persisted? r)) "never success-shaped: not in qdrant yet")
      (is (= :queued-not-persisted (:status r)))
      (is (some #(str/starts-with? % "NOT PERSISTED YET") (:tips r))))
    (q/enqueue! {:op :delete-entry! :id "b" :args ["b"] :collection "c"})
    (testing "a new process starts with what the old one queued"
      (let [summary (attach-fresh! path)]
        (is (= 2 (:recovered summary)))
        (is (= 2 (q/size)))
        (is (= ["a" "b"] (ids)))))))

(deftest recovered-ops-go-ahead-of-ops-queued-since
  (let [path (temp-path)]
    (attach-fresh! path)
    (q/enqueue! {:op :add-entry! :id "old" :args [{:id "old"}]})
    (q/detach-outbox!)
    (q/clear!)
    (q/enqueue! {:op :add-entry! :id "new" :args [{:id "new"}]})
    (q/attach-outbox! (outbox/file-outbox path) path)
    (let [seen (atom [])]
      (q/drain! {:dispatch-fn #(swap! seen conj (:id %))})
      (is (= ["old" "new"] @seen)))))

(deftest a-drained-op-leaves-the-disk-a-failed-one-stays
  (let [path (temp-path)]
    (attach-fresh! path)
    (q/enqueue! {:op :add-entry! :id "ok" :args [{:id "ok"}]})
    (q/enqueue! {:op :add-entry! :id "bad" :args [{:id "bad"}]})
    (q/drain! {:dispatch-fn (fn [op] (when (= "bad" (:id op)) (throw (Exception. "boom"))))})
    (is (= ["bad"] (ids)))
    (q/drain! {:dispatch-fn (constantly :ok)})
    (is (= [] (ids)))))

(deftest a-torn-last-line-costs-only-itself
  (let [path (temp-path)]
    (attach-fresh! path)
    (q/enqueue! {:op :delete-entry! :id "kept" :args ["kept"]})
    (spit path "{:op :add-entry! :id \"torn\" :args [{:id" :append true)
    (let [summary (attach-fresh! path)]
      (is (= 1 (:recovered summary)))
      (is (= 1 (:corrupt summary)))
      (is (= ["kept"] (ids)) "the rewrite on attach drops the torn line"))))

(deftest attaching-the-same-path-twice-recovers-once
  (let [path (temp-path)]
    (attach-fresh! path)
    (q/enqueue! {:op :delete-entry! :id "x" :args ["x"]})
    (attach-fresh! path)
    (q/attach-outbox! (outbox/file-outbox path) path)
    (is (= 1 (q/size)))))

(deftest concurrent-enqueues-all-reach-the-disk
  (let [path (temp-path)]
    (attach-fresh! path)
    (let [n 200]
      (->> (range n)
           (mapv (fn [i] (future (q/enqueue! {:op :delete-entry! :id (str i) :args [(str i)]}))))
           (run! deref))
      (is (= n (count (ids))))
      (is (= (set (map str (range n))) (set (ids)))))))

(deftest enqueue-during-a-drain-is-not-lost-on-disk
  (let [path  (temp-path)
        first? (atom true)]
    (attach-fresh! path)
    (q/enqueue! {:op :delete-entry! :id "a" :args ["a"]})
    (q/drain! {:dispatch-fn (fn [_]
                              (when (compare-and-set! first? true false)
                                (q/enqueue! {:op :delete-entry! :id "mid" :args ["mid"]})
                                (throw (Exception. "boom"))))})
    (is (= #{"a" "mid"} (set (ids))))))

;; ---- RAM-only and failing disk ----------------------------------------------

(defrecord DeadDisk []
  outbox/IOutbox
  (append! [_ _] false)
  (rewrite! [_ _] false)
  (load-ops [_] {:ops [] :corrupt 0}))

(deftest without-an-outbox-the-answer-says-ram-only
  (let [r (q/enqueue! {:op :delete-entry! :id "r" :args ["r"]})]
    (is (false? (:spilled? r)))
    (is (false? (:durable? (q/stats))))
    (is (some #(str/includes? % "LOSES") (:tips r)))))

(deftest a-failing-disk-still-queues-and-says-so
  (q/attach-outbox! (->DeadDisk) "dead")
  (let [r (q/enqueue! {:op :delete-entry! :id "d" :args ["d"]})]
    (is (:queued? r))
    (is (false? (:spilled? r)))
    (is (= 1 (q/size)))))

;; ---- routing -----------------------------------------------------------------

(deftest routed-dispatch-sends-an-op-to-its-collection
  (let [seen (atom [])]
    (q/register-dispatcher! "c1" #(swap! seen conj [:c1 (:id %)]))
    (try
      (q/routed-dispatch {:op :delete-entry! :id "x" :collection "c1"})
      (is (= [[:c1 "x"]] @seen))
      (is (thrown? clojure.lang.ExceptionInfo
                   (q/routed-dispatch {:op :delete-entry! :id "y" :collection "nope"}))
          "no store: the op stays queued")
      (finally (q/unregister-dispatcher! "c1")))))

(deftest one-id-in-two-collections-is-two-entries
  (is (= 2 (count (q/coalesce [{:op :delete-entry! :id "k" :args ["k"] :collection "a"}
                               {:op :delete-entry! :id "k" :args ["k"] :collection "b"}])))))

(deftest file-outbox-rewrite-is-whole
  (let [path (temp-path)
        ob   (outbox/file-outbox path)]
    (outbox/append! ob {:op :delete-entry! :id "1" :args ["1"]})
    (outbox/rewrite! ob [{:op :delete-entry! :id "2" :args ["2"]}])
    (is (= ["2"] (mapv :id (:ops (outbox/load-ops ob)))))
    (is (not (.exists (io/file (str path ".tmp")))))))
