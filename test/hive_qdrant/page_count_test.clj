(ns hive-qdrant.page-count-test
  "IMemoryStoreCount / IMemoryStorePage on QdrantMemoryStore: the live branch
   over the fake client (one scroll per page, the cursor qdrant hands back,
   the payload projection on the wire) and the in-memory fallback."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-qdrant.circuit :as circuit]
            [hive-qdrant.fake-qdrant :as fake]
            [hive-qdrant.store :as store]
            [hive-spi.memory.conformance :as conformance]
            [hive-spi.memory.ports :as proto]))

(defn- fallback-store []
  (circuit/force-reset!)
  (store/create-store {:vector-size 4}))

(defn- live-store
  ([] (live-store (fake/client {})))
  ([client]
   (circuit/force-reset!)
   (let [s (store/create-store {:collection-name "test-page" :vector-size 4})]
     (reset! (:client-atom s) {:client client})
     (reset! (:connected?-atom s) true)
     s)))

(def ^:private port? (some? (ns-resolve 'hive-spi.memory.ports 'IMemoryStorePage)))

(defn- count-entries [s o] ((ns-resolve 'hive-spi.memory.ports 'count-entries) s o))
(defn- query-page [s o] ((ns-resolve 'hive-spi.memory.ports 'query-page) s o))

(defn- seed! [s n]
  (doseq [i (range n)]
    (proto/add-entry! s {:id (format "%014d-0000000%d" i (mod i 10)) :type "note"
                         :tags ["carto"] :project-id "p" :content (str "row " i)})))

(defn- walk [s opts]
  (loop [cursor nil, rows [], calls 0]
    (let [r (query-page s (assoc opts :cursor cursor))]
      (if (and (:next-cursor r) (< calls 100))
        (recur (:next-cursor r) (into rows (:rows r)) (inc calls))
        [(into rows (:rows r)) (inc calls)]))))

(deftest live-count-and-page
  (when port?
    (let [s (live-store)]
      (seed! s 7)
      (is (= 7 (count-entries s {:tags ["carto"]})))
      (let [[rows calls] (walk s {:limit 3 :tags ["carto"]})]
        (is (= 7 (count rows)))
        (is (= 7 (count (distinct (map :id rows)))) "each point once")
        (is (= 3 calls) "7 rows at 3 a page is three round-trips"))
      (testing "the projection reaches qdrant: only the asked payload keys come back"
        (let [r (query-page s {:limit 10 :output-fields ["type"]})]
          (is (= 7 (count (:rows r))))
          (is (every? #(= #{:id :type} (set (keys %))) (:rows r)))
          (is (nil? (:next-cursor r))))))))

(deftest live-failure-is-not-an-answer
  (when port?
    (let [s (live-store (fake/broken-client "FATAL: boom"))]
      (is (thrown? clojure.lang.ExceptionInfo (count-entries s {})) "a failed count is not 0")
      (is (thrown? clojure.lang.ExceptionInfo (query-page s {})) "a failed page is not empty"))))

(deftest fallback-count-and-page
  (when port?
    (let [s (fallback-store)]
      (seed! s 5)
      (is (= 5 (count-entries s {})))
      (let [[rows _] (walk s {:limit 2 :output-fields ["type"]})]
        (is (= 5 (count (distinct (map :id rows)))))
        (is (not-any? #(contains? % :content) rows))))))

(deftest build-payload-keys-honours-output-fields
  (let [k #'store/build-payload-keys]
    (is (= ["id" "type"] (k {:output-fields ["type"]})))
    (is (= ["id" "tags"] (k {:output-fields ["tags" "id"]})))
    (is (= ["id" "type" "tags"] (k {:output-fields ["type"] :exclude-tags ["x"]}))
        "exclude-tags reads :tags client-side, so the projection keeps it")
    (is (some #{"content"} (k {:include-content? true})))
    (is (not-any? #{"content"} (k {})) "no projection: the default hot-path keys")))

(deftest conformance-count-and-page
  ;; Fallback only: the fake client ignores request filters (see fake-qdrant),
  ;; and expiry is not filterable server-side on a live qdrant, so the filter
  ;; cases are only meaningful where the store itself evaluates them.
  (when port?
    (doseq [id [:count :page]]
      (is (= :ran (conformance/run-case fallback-store {} id)) (str id)))))
