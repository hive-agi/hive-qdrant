(ns hive-qdrant.scan-test
  "IMemoryStoreScan over the in-memory fallback: every id once, expired ids
   only on request. Bound only when the hive-spi on the classpath declares
   the port; against an older hive-spi the store must not claim it."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.store :as store]
            [hive-spi.memory.ports :as proto]))

(def ^:private scan-port
  (some-> (ns-resolve 'hive-spi.memory.ports 'IMemoryStoreScan) deref))

(def ^:private scan-ids
  (some-> (ns-resolve 'hive-spi.memory.ports 'scan-ids) deref))

(deftest expired-at-reads-both-timestamp-shapes
  (let [expired-at? #'store/expired-at?
        now         (java.time.Instant/parse "2026-09-27T12:00:00Z")]
    (is (expired-at? "2026-09-26T12:00:00Z" now))
    (is (expired-at? "2026-09-27T08:59:00-03:00[America/Bahia]" now))
    (is (not (expired-at? "2026-10-03T02:36:41.937358138-03:00[America/Bahia]" now)))
    (is (not (expired-at? "" now)))
    (is (not (expired-at? nil now)))
    (is (not (expired-at? "not a date" now)) "unparsable keeps the row")))

(deftest scan-lists-every-id-once
  (let [s (store/create-store {})]
    (if-not scan-port
      (testing "an older hive-spi: the store claims no scan"
        (is (nil? scan-ids)))
      (do
        (is (satisfies? scan-port s))
        (is (empty? (scan-ids s {})) "an empty store scans to nothing")
        (doseq [i (range 5)]
          (proto/add-entry! s {:id (str "live-" i) :type "note" :content (str "row " i)}))
        (proto/add-entry! s {:id "old" :type "note" :content "gone"
                             :expires "2000-01-01T00:00:00Z"})
        (let [ids (scan-ids s {})]
          (is (= (count ids) (count (distinct ids))) "no id twice")
          (is (= (set (map #(str "live-" %) (range 5))) (set ids))))
        (is (contains? (set (scan-ids s {:include-expired? true})) "old"))))))

(deftest a-scroll-must-agree-with-the-count-before-and-after
  (let [stable-scroll #'store/stable-scroll
        points        (vec (range 4))]
    (testing "a steady collection is read once"
      (let [calls (atom 0)]
        (is (= points (stable-scroll (constantly 4) #(do (swap! calls inc) points) 5)))
        (is (= 1 @calls))))
    (testing "a collection that moved during the scroll is read again"
      (let [counts  (atom [4 5 5 5])
            count!  #(let [c (first @counts)] (swap! counts rest) c)
            scrolls (atom [points (conj points 4)])
            scroll! #(let [s (first @scrolls)] (swap! scrolls rest) s)]
        (is (= 5 (count (stable-scroll count! scroll! 5))))))
    (testing "a scroll that always comes back short raises instead of passing"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"never matched"
                            (stable-scroll (constantly 5) (constantly points) 2))))))

(deftest drift-backs-off-exponentially-and-is-capped
  (let [backoff #'store/drift-backoff-ms]
    (is (= [250 500 1000 2000 4000 8000 8000] (mapv backoff [1 2 3 4 5 6 7])))
    (is (= 8000 (backoff 1000)) "a huge attempt number cannot overflow the cap")))

(deftest a-drifting-scroll-waits-longer-each-time
  (let [stable-scroll #'store/stable-scroll
        points        (vec (range 4))
        sleeps        (atom [])]
    (testing "a steady collection never sleeps"
      (stable-scroll (constantly 4) (constantly points) 5 #(swap! sleeps conj %))
      (is (empty? @sleeps)))
    (testing "each re-read follows a longer pause; the last failure does not sleep"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"never matched"
                            (stable-scroll (constantly 5) (constantly points) 4
                                           #(swap! sleeps conj %))))
      (is (= [250 500 1000] @sleeps)))))

(deftest a-collection-that-never-holds-still-answers-its-snapshot
  (let [stable-scroll #'store/stable-scroll
        sleeps        (atom [])
        sleep!        #(swap! sleeps conj %)
        ;; every attempt: count 10, scroll, count 12 — steady writes
        counter       (atom 0)
        count!        #(if (even? (swap! counter inc)) 12 10)]
    (testing "a scroll inside the before/after band is answered once attempts run out"
      (let [points (stable-scroll count! (constantly (vec (range 11))) 3 sleep!)]
        (is (= 11 (count points)))
        (is (= [250 500] @sleeps) "it still backed off before giving in")))
    (testing "a scroll outside the band is lossy and raises"
      (reset! counter 0)
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"never matched"
                            (stable-scroll count! (constantly (vec (range 3))) 3 sleep!))))
    (testing "the band is inclusive at both ends and direction-free"
      (let [within? #'store/within-drift?]
        (is (within? 10 12 10))
        (is (within? 10 12 12))
        (is (within? 12 10 11) "a shrinking collection too")
        (is (not (within? 10 12 13)))
        (is (not (within? 10 12 9)))))))

(deftest a-paged-scroll-reads-every-page-and-stops
  (let [page-through #'store/page-through
        pages        {nil {:points [1 2] :next-offset :a}
                      :a  {:points [3 4] :next-offset :b}
                      :b  {:points [5]   :next-offset nil}}]
    (is (= [1 2 3 4 5] (page-through pages)))
    (is (= [] (page-through (constantly {:points [] :next-offset :x}))) "an empty page ends it")
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"did not advance"
                          (page-through (constantly {:points [1] :next-offset :same})))
        "an offset that never advances raises instead of looping")))

(deftest live-scan-pages-through-the-collection
  (when scan-port
    (circuit/force-reset!)
    (let [s (store/create-store {:collection-name "test-scan" :vector-size 4})]
      (reset! (:client-atom s) {:client (fake/client {})})
      (reset! (:connected?-atom s) true)
      (let [n 1300] ; > 2 pages of 512
        (doseq [i (range n)]
          (proto/add-entry! s {:id (format "%014d-0000000%d" i (mod i 10)) :type "note"
                               :content (str "row " i)}))
        (let [ids (scan-ids s {})]
          (is (= n (count ids)))
          (is (= n (count (set ids)))))))))
