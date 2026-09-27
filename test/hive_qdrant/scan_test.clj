(ns hive-qdrant.scan-test
  "IMemoryStoreScan over the in-memory fallback: every id once, expired ids
   only on request. Bound only when the hive-spi on the classpath declares
   the port; against an older hive-spi the store must not claim it."
  (:require [clojure.test :refer [deftest is testing]]
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
