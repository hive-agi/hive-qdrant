(ns hive-qdrant.store-embed-text-test
  "QdrantMemoryStore honours the hive-spi :embed-text capability
   (hive-spi.memory.decorate): a transient :embed-text is embedded in place
   of :content and never persisted.

   Why it matters: hive-knowledge's seal encrypts :content at rest and hands
   the backend the plaintext it may index on :embed-text. A store that embeds
   :content indexes ciphertext; a store that persists :embed-text writes the
   plaintext the seal just encrypted into the payload.

   No live qdrant and no redefs: the embedder is a recording fn injected
   through the store config, and the client is hive-qdrant.fake-qdrant, which
   keeps the points the store upserts, so the tests read the payload that
   would have gone over the wire."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clj-qdrant.api :as q-api]
            [hive-spi.memory.conformance :as conformance]
            [hive-spi.memory.decorate :as decorate]
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

(def ^:private vsize 4)

(defn- text->vector
  "Deterministic embedding: 'a' count, 'z' count, a bias, 0. Texts that differ
   in those letters get distinguishable vectors."
  [text]
  [(double (count (filter #{\a} text)))
   (double (count (filter #{\z} text)))
   1.0
   0.0])

(defn- recording-embedder
  "[seen embedder]: `embedder` embeds with `text->vector` and appends every
   text it is handed to the `seen` atom."
  []
  (let [seen (atom [])]
    [seen (fn [text] (swap! seen conj text) (text->vector text))]))

(defn- live-store
  "A store on its LIVE branch over a fresh fake client. Returns [store client].
   CLIENT-OPTS go to fake/client ({:vector-field :data}: a pre-dense server)."
  ([] (live-store {}))
  ([extra] (live-store extra {}))
  ([extra client-opts]
   (let [s (store/create-store (merge {:collection-name "test-embed-text"
                                       :vector-size     vsize}
                                      extra))
         c (fake/client client-opts)]
     (reset! (:client-atom s) {:client c})
     (reset! (:connected?-atom s) true)
     [s c])))

;; =============================================================================
;; Capability
;; =============================================================================

(deftest store-status-declares-embed-text
  (let [status (proto/store-status (store/create-store {:vector-size vsize}))]
    (is (some #{:embed-text} (:capabilities status)))
    (is (decorate/embed-text-capable? status)
        "the check hive-knowledge's seal runs before it will wrap a store")
    (testing "the existing keys survive"
      (is (every? #(contains? status %)
                  [:backend :connected? :collection-name :vector-size :circuit :queue])))))

;; =============================================================================
;; add-entry!
;; =============================================================================

(deftest add-entry-embeds-the-embed-text-not-the-content
  (let [[seen embedder] (recording-embedder)
        [s c]           (live-store {:embedder embedder})]
    (is (= "e1" (proto/add-entry! s {:id "e1" :type :note
                                     :content "CIPHERTEXT" :embed-text "zzzz"})))
    (is (= ["zzzz"] @seen) "the embedder must never see the sealed content")
    (is (= (text->vector "zzzz") (fake/point-vector c "e1")))
    (let [payload (fake/payload c "e1")]
      (is (not (contains? payload :embed-text)) "the plaintext never reaches the payload")
      (is (= "CIPHERTEXT" (:content payload)) ":content is stored as given"))
    (is (not (contains? (proto/get-entry s "e1") :embed-text)))))

(deftest add-entry-without-embed-text-embeds-content
  (let [[seen embedder] (recording-embedder)
        [s c]           (live-store {:embedder embedder})]
    (proto/add-entry! s {:id "e1" :type :note :content "aaaa" :tags ["t"]})
    (proto/add-entry! s {:id "e2" :type :note :content {:task "map content"}})
    (is (= ["aaaa" (pr-str {:task "map content"})] @seen)
        "string content as is, native content pr-str'd: unchanged")
    (is (= (text->vector "aaaa") (fake/point-vector c "e1")))
    (is (= {:id "e1" :type "note" :content "aaaa" :tags ["t"]} (fake/payload c "e1")))))

(deftest add-entry-given-blank-embed-text-never-falls-back-to-content
  (testing "a given :embed-text is what may be indexed, even when blank"
    (let [[seen embedder] (recording-embedder)
          [s c]           (live-store {:embedder embedder})
          res             (proto/add-entry! s {:id "e1" :type :note
                                               :content "CIPHERTEXT" :embed-text " "})]
      (is (= :qdrant/embed-failed (:error res)))
      (is (empty? @seen) "falling back to :content would index ciphertext")
      (is (nil? (fake/payload c "e1")) "nothing is written"))))

(deftest add-entry-on-a-structural-slot-still-strips-embed-text
  (testing "no embedder (the :kanban slot): placeholder vector, no :embed-text"
    (let [[s c] (live-store)]
      (is (= "k1" (proto/add-entry! s {:id "k1" :type :task
                                       :content "CIPHERTEXT" :embed-text "plain"})))
      (is (not (contains? (fake/payload c "k1") :embed-text))))))

(deftest fallback-never-stores-embed-text
  (let [s (store/create-store {:vector-size vsize})]
    (proto/add-entry! s {:id "f1" :type :note :content "CIPHERTEXT" :embed-text "plain"})
    (is (= "CIPHERTEXT" (:content (proto/get-entry s "f1"))))
    (is (not (contains? (proto/get-entry s "f1") :embed-text)))
    (is (not-any? #(contains? % :embed-text) (vals (:entries @(:fallback-atom s)))))))

;; =============================================================================
;; update-entry!
;; =============================================================================

(deftest update-entry-re-embeds-from-embed-text
  (let [[seen embedder] (recording-embedder)
        [s c]           (live-store {:embedder embedder})]
    (proto/add-entry! s {:id "u1" :type :note :content "aaaa" :tags ["t"]})
    (reset! seen [])
    (testing ":content and :embed-text: the embed text is embedded"
      (proto/update-entry! s "u1" {:content "CIPHER-2" :embed-text "zz"})
      (is (= ["zz"] @seen))
      (is (= (text->vector "zz") (fake/point-vector c "u1")))
      (is (= "CIPHER-2" (:content (fake/payload c "u1"))))
      (is (not (contains? (fake/payload c "u1") :embed-text))))
    (testing ":embed-text alone re-embeds and leaves :content as stored"
      (reset! seen [])
      (proto/update-entry! s "u1" {:embed-text "zzzz"})
      (is (= ["zzzz"] @seen))
      (is (= (text->vector "zzzz") (fake/point-vector c "u1")))
      (is (= "CIPHER-2" (:content (fake/payload c "u1"))))
      (is (= ["t"] (:tags (fake/payload c "u1"))))
      (is (not (contains? (fake/payload c "u1") :embed-text))))))

(deftest update-entry-without-embed-text-is-unchanged
  (let [[seen embedder] (recording-embedder)
        [s c]           (live-store {:embedder embedder})]
    (proto/add-entry! s {:id "u1" :type :note :content "aaaa"})
    (reset! seen [])
    (testing ":content alone embeds the new :content"
      (proto/update-entry! s "u1" {:content "aa"})
      (is (= ["aa"] @seen))
      (is (= (text->vector "aa") (fake/point-vector c "u1"))))
    (testing "neither: the stored :content is embedded again"
      (reset! seen [])
      (proto/update-entry! s "u1" {:tags ["x"]})
      (is (= ["aa"] @seen))
      (is (= ["x"] (:tags (fake/payload c "u1")))))))

(deftest a-leaked-embed-text-is-neither-read-nor-re-embedded
  (testing "a point written before the capability may carry :embed-text in its payload"
    (let [[seen embedder] (recording-embedder)
          [s c]           (live-store {:embedder embedder})]
      (q-api/upsert-points {:client c} :collection "test-embed-text"
                           :points [{:id      (str (java.util.UUID/nameUUIDFromBytes
                                                    (.getBytes "old" "UTF-8")))
                                     :vector  [0.0 1.0 1.0 0.0]
                                     :payload {:id "old" :type "note" :content "aaaa"
                                               :embed-text "leaked plaintext"}}])
      (is (not (contains? (proto/get-entry s "old") :embed-text)) "no read surfaces it")
      (proto/update-entry! s "old" {:content "aa"})
      (is (= ["aa"] @seen) "the stale embed text must not win over new content")
      (is (not (contains? (fake/payload c "old") :embed-text)) "the rewrite drops it"))))

;; =============================================================================
;; update-metadata!
;; =============================================================================

(deftest update-metadata-keeps-the-vector-and-never-stores-embed-text
  (let [[seen embedder] (recording-embedder)
        [s c]           (live-store {:embedder embedder})]
    (proto/add-entry! s {:id "m1" :type :note :content "CIPHERTEXT" :embed-text "zzzz"})
    (reset! seen [])
    (testing "a metadata write does not re-embed"
      (proto/update-metadata! s "m1" {:tags ["x"]})
      (is (empty? @seen) "re-embedding here would index the sealed content")
      (is (= (text->vector "zzzz") (fake/point-vector c "m1")))
      (is (= ["x"] (:tags (fake/payload c "m1")))))
    (testing "an :embed-text asks for a new vector, embedded from that text"
      (proto/update-metadata! s "m1" {:tags ["y"] :embed-text "zz"})
      (is (= ["zz"] @seen))
      (is (= (text->vector "zz") (fake/point-vector c "m1")))
      (is (not (contains? (fake/payload c "m1") :embed-text))))))

(defn- retrieved-vector-of
  "store/retrieved-vector over a RetrievedPoint whose vectors output is VO."
  [vo]
  (#'store/retrieved-vector
   (fake/retrieved-point (#'q-api/->point {:id     (str (java.util.UUID/randomUUID))
                                           :vector [0.0 0.0 0.0 0.0]})
                         vo)))

(deftest retrieved-vector-reads-dense-first-then-data
  (testing "qdrant 1.17 answers a retrieve on `dense`, the `data` field empty"
    (is (= [1.0 2.0 3.0 4.0] (retrieved-vector-of (fake/vector-output :dense [1 2 3 4])))
        "reading `data` alone found nothing here, and every metadata write re-embedded"))
  (testing "a server that still fills the deprecated `data` field"
    (is (= [1.0 2.0 3.0 4.0] (retrieved-vector-of (fake/vector-output :data [1 2 3 4])))))
  (testing "no vector at all: nil, which routes to the embedding path"
    (is (nil? (retrieved-vector-of nil)))
    (is (nil? (retrieved-vector-of (fake/vector-output :dense []))))))

(deftest update-metadata-finds-the-stored-vector-on-either-field
  (doseq [field [:dense :data]]
    (testing (str "retrieved vectors on the " (name field) " field")
      (let [[seen embedder] (recording-embedder)
            [s c]           (live-store {:embedder embedder} {:vector-field field})]
        (proto/add-entry! s {:id "m1" :type :note :content "CIPHERTEXT"
                             :embed-text "zzzz" :tags ["t"]})
        (reset! seen [])
        (let [merged (proto/update-metadata! s "m1" {:tags ["x"]})]
          (is (empty? @seen) "the stored vector was found: nothing is re-embedded")
          (is (= ["x"] (:tags merged))))
        (is (= (text->vector "zzzz") (fake/point-vector c "m1")))
        (is (= "CIPHERTEXT" (:content (fake/payload c "m1"))))
        (is (= ["x"] (:tags (fake/payload c "m1"))))))))

(deftest update-metadata-type-change-keeps-the-vector
  (testing "the vector is a function of the embed text alone; :type is metadata"
    (let [[seen embedder] (recording-embedder)
          [s c]           (live-store {:embedder embedder})]
      (proto/add-entry! s {:id "t1" :type :note :content "CIPHERTEXT" :embed-text "zzzz"})
      (reset! seen [])
      (proto/update-metadata! s "t1" {:type :decision})
      (is (empty? @seen) "re-embedding a :type change would index the sealed :content")
      (is (= (text->vector "zzzz") (fake/point-vector c "t1")))
      (is (= "decision" (:type (fake/payload c "t1"))))
      (is (= "CIPHERTEXT" (:content (fake/payload c "t1"))))
      (testing "a non-string :embed-text asks for no new vector and is not stored"
        (proto/update-metadata! s "t1" {:tags ["y"] :embed-text 42})
        (is (empty? @seen))
        (is (= (text->vector "zzzz") (fake/point-vector c "t1")))
        (is (= ["y"] (:tags (fake/payload c "t1"))))
        (is (not (contains? (fake/payload c "t1") :embed-text))))
      (testing ":content still changes the vector"
        (proto/update-metadata! s "t1" {:content "aa"})
        (is (= ["aa"] @seen))
        (is (= (text->vector "aa") (fake/point-vector c "t1")))
        (is (= "decision" (:type (fake/payload c "t1"))))))))

;; =============================================================================
;; Every spelling of the key that lands in the "embed-text" payload field
;; =============================================================================

(def ^:private other-embed-text-keys
  "Keys clj-qdrant's ->payload writes to the field \"embed-text\" (it names
   every key with `name`), other than the unqualified :embed-text."
  ["embed-text" :x/embed-text :hive-knowledge.seal/embed-text 'embed-text])

(defn- names-embed-text? [m]
  (boolean (some #(= "embed-text" (name %)) (keys m))))

(deftest no-spelling-of-embed-text-reaches-a-payload
  (doseq [k other-embed-text-keys]
    (testing (pr-str k)
      (let [[seen embedder] (recording-embedder)
            [s c]           (live-store {:embedder embedder})]
        (testing "add-entry!"
          (proto/add-entry! s {:id "s1" :type :note :content "aaaa" k "plain"})
          (is (not (contains? (fake/payload c "s1") :embed-text)))
          (is (= ["aaaa"] @seen) "only the unqualified :embed-text is embedding input"))
        (testing "update-entry!"
          (proto/update-entry! s "s1" {:tags ["u"] k "plain"})
          (is (not (contains? (fake/payload c "s1") :embed-text)))
          (is (= ["u"] (:tags (fake/payload c "s1")))))
        (testing "update-metadata!: the key is dropped, the vector kept"
          (reset! seen [])
          (proto/update-metadata! s "s1" {:tags ["m"] k "plain"})
          (is (empty? @seen))
          (is (not (contains? (fake/payload c "s1") :embed-text)))
          (is (= ["m"] (:tags (fake/payload c "s1")))))
        (testing "no read surfaces it"
          (is (not (names-embed-text? (proto/get-entry s "s1")))))))))

(deftest no-spelling-of-embed-text-reaches-the-fallback
  (doseq [k other-embed-text-keys]
    (testing (pr-str k)
      (let [s      (store/create-store {:vector-size vsize})
            stored #(get-in @(:fallback-atom s) [:entries "f1"])]
        (proto/add-entry! s {:id "f1" :type :note :content "CIPHERTEXT" k "plain"})
        (is (not (names-embed-text? (stored))) "add-entry!")
        (proto/update-entry! s "f1" {:tags ["u"] k "plain"})
        (is (not (names-embed-text? (stored))) "update-entry!")
        (proto/update-metadata! s "f1" {:tags ["m"] k "plain"})
        (is (not (names-embed-text? (stored))) "update-metadata!")
        (is (= "CIPHERTEXT" (:content (stored))))
        (is (= ["m"] (:tags (stored))))))))

;; =============================================================================
;; Queue replay
;; =============================================================================

(deftest a-queued-write-replays-with-its-embed-text
  (testing "an open circuit queues the write; the replay embeds :embed-text"
    (let [[seen embedder] (recording-embedder)
          [s c]           (live-store {:embedder embedder})]
      (circuit/configure! {:threshold 1 :cooldown-ms 60000})
      (circuit/record-failure!)
      (is (:queued? (proto/add-entry! s {:id "q1" :type :note
                                         :content "CIPHERTEXT" :embed-text "zzzz"})))
      (circuit/force-reset!)
      (q/drain! {:dispatch-fn (q/make-store-dispatch s)})
      (is (= ["zzzz"] @seen))
      (is (= "CIPHERTEXT" (:content (fake/payload c "q1"))))
      (is (not (contains? (fake/payload c "q1") :embed-text))))))

;; =============================================================================
;; Semantic search ranks by what was embedded
;; =============================================================================

(deftest search-ranks-by-the-embed-text
  (testing "the conformance :embed-text claim, read through qdrant's result envelope"
    (let [[_ embedder] (recording-embedder)
          [s _]        (live-store {:embedder embedder})]
      (proto/add-entry! s {:id "hidden" :type :note :content "aaaaaaaa" :embed-text "zzzzzzzz"})
      (proto/add-entry! s {:id "plain" :type :note :content "aaaaaaaa"})
      (is (= "hidden" (:id (first (:results (proto/search-similar s "zzzzzzzz" {:limit 2}))))))
      (is (= "plain" (:id (first (:results (proto/search-similar s "aaaaaaaa" {:limit 2})))))))))

;; =============================================================================
;; hive-spi conformance
;; =============================================================================

(deftest conformance-embed-text-on-the-fallback
  (is (= :ran (conformance/run-case #(store/create-store {:vector-size vsize}) {} :embed-text))))

(deftest conformance-embed-text-on-the-live-branch
  ;; Without an embedder: the case's semantic half assumes search-similar
  ;; returns a sequential, and this store answers a {:success? :results}
  ;; envelope, which `search-ranks-by-the-embed-text` covers instead.
  (is (= :ran (conformance/run-case #(first (live-store)) {} :embed-text))))
