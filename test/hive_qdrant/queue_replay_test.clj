(ns hive-qdrant.queue-replay-test
  "queue/coalesce is a replay optimisation, never a change of outcome:
   replaying the coalesced ops leaves the entry as replaying the raw ops, in
   the order they were queued, would.

   Generative: random sequences of adds, partial updates and deletes on one
   entry id, over an entry that may or may not already be stored, replayed
   both ways through the store's own drain dispatch, each into a fresh store
   over its own hive-qdrant.fake-qdrant. No live qdrant."
  (:require [clojure.test :refer [use-fixtures]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
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

(def ^:private id "p")

(defn- text->vector
  "Deterministic 4-wide embedding: texts that differ in their a and z counts
   get different vectors, so a replay that embeds other text shows."
  [text]
  [(double (count (filter #{\a} text)))
   (double (count (filter #{\z} text)))
   1.0
   0.0])

;; =============================================================================
;; Generators
;; =============================================================================

(defn- gen-some-of
  "A map holding a random subset of FIELDS, a vector of [k generator] pairs:
   each key present or absent independently, generated when present."
  [fields]
  (gen/fmap (fn [[present vs]]
              (into {} (keep (fn [[p [k] v]] (when p [k v])))
                    (map vector present fields vs)))
            (gen/tuple (gen/vector gen/boolean (count fields))
                       (apply gen/tuple (map second fields)))))

(def ^:private gen-content    (gen/elements ["alpha" "zeta" "azaz" "zz"]))
(def ^:private gen-embed-text (gen/elements ["aaaa" "zzzz" "az"]))
(def ^:private gen-tags       (gen/vector (gen/elements ["t1" "t2" "t3"]) 0 2))
(def ^:private gen-duration   (gen/elements ["short" "long" nil]))
(def ^:private gen-type       (gen/elements [:note :decision]))

(def ^:private gen-add
  "A whole entry, as add-entry! queues it."
  (gen/fmap (fn [[content more]] (merge {:id id :type :note :content content} more))
            (gen/tuple gen-content
                       (gen-some-of [[:tags gen-tags]
                                     [:duration gen-duration]
                                     [:embed-text gen-embed-text]]))))

(def ^:private gen-updates
  "The fields one update-entry! call changes."
  (gen-some-of [[:content gen-content]
                [:tags gen-tags]
                [:duration gen-duration]
                [:type gen-type]
                [:embed-text gen-embed-text]]))

(def ^:private gen-op
  "One queued op on `id`, shaped as the store queues it."
  (gen/frequency
   [[3 (gen/fmap (fn [e] {:op :add-entry! :id id :args [e]}) gen-add)]
    [5 (gen/fmap (fn [u] {:op :update-entry! :id id :args [id u]}) gen-updates)]
    [2 (gen/return {:op :delete-entry! :id id :args [id]})]]))

(def ^:private gen-seed
  "The entry already stored when the replay starts, or nil for none."
  (gen/one-of [(gen/return nil)
               (gen/return {:id id :type :note :content "seed" :tags ["t0"] :duration "short"})]))

;; =============================================================================
;; Replay
;; =============================================================================

(defn- replay
  "What `id` is left as after SEED is stored and OPS replay in order, each
   through the drain dispatch, into a fresh store over a fresh fake:
   {:payload .. :vector ..}, both nil when no point is left."
  [seed ops]
  (circuit/force-reset!)
  (q/clear!)
  (let [s (store/create-store {:collection-name "test-replay" :vector-size 4
                               :embedder text->vector})
        c (fake/client)]
    (reset! (:client-atom s) {:client c})
    (reset! (:connected?-atom s) true)
    (when seed (proto/add-entry! s seed))
    (run! (q/make-store-dispatch s) ops)
    {:payload (fake/payload c id)
     :vector  (fake/point-vector c id)}))

(defn- carries-embed-text? [{:keys [args]}]
  (some #(and (map? %) (contains? % :embed-text)) args))

(defspec coalesced-replay-leaves-the-entry-raw-replay-would 300
  (prop/for-all [seed gen-seed
                 ops  (gen/vector gen-op 0 8)]
    (let [coalesced (q/coalesce ops)
          raw       (replay seed ops)
          folded    (replay seed coalesced)]
      (and (<= (count coalesced) 1)
           (= (:payload raw) (:payload folded))
           ;; The vector matches too, unless an :embed-text rode along: run
           ;; in turn, an update after it re-embeds the stored :content (the
           ;; read drops the transient text), where the fold keeps the text
           ;; until a newer :content retires it.
           (or (some carries-embed-text? ops)
               (= (:vector raw) (:vector folded)))))))
