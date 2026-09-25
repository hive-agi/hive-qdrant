(ns hive-qdrant.queue-test
  "Trifecta for write queue: golden + property + mutation."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-qdrant.queue :as q]))

(use-fixtures :each (fn [f] (q/clear!) (f) (q/clear!)))

;; ---- Golden ---------------------------------------------------------------

(deftest golden-enqueue-accept
  (let [r (q/enqueue! {:op :add-entry! :id "a" :args [{:id "a"}]})]
    (is (:success? r))
    (is (:queued? r))
    (is (= 1 (:depth r)))
    (is (vector? (:tips r)))))

(deftest golden-degraded-response-shape
  (let [r (q/degraded-response {:operation :get-entry})]
    (is (false? (:success? r)))
    (is (true?  (:degraded? r)))
    (is (= "qdrant" (:backend r)))
    (is (true?  (:reconnecting? r)))
    (is (vector? (:tips r)))))

(deftest golden-coalesce-collapses-same-id
  (let [ops [{:op :add-entry! :id "a" :args [{:v 1}]}
             {:op :add-entry! :id "b" :args [{:v 1}]}
             {:op :add-entry! :id "a" :args [{:v 2}]}]
        out (q/coalesce ops)]
    (is (= 2 (count out)))
    (is (= 2 (-> (first (filter #(= "a" (:id %)) out)) :args first :v)))))

;; ---- Property -------------------------------------------------------------

(deftest property-fifo-order-preserved
  (let [ids (mapv str (range 10))
        ops (mapv (fn [i] {:op :add-entry! :id i :args [{:id i}]}) ids)]
    (doseq [op ops] (q/enqueue! op))
    (is (= 10 (q/size)))
    (let [coalesced (q/coalesce ops)]
      (is (= ids (mapv :id coalesced))))))

(deftest property-drain-all-success
  (doseq [i (range 5)]
    (q/enqueue! {:op :add-entry! :id (str i) :args [{:id (str i)}]}))
  (let [log (atom [])
        r   (q/drain! {:dispatch-fn (fn [op] (swap! log conj (:id op)) :ok)})]
    (is (= :drained (:result r)))
    (is (= 5 (:succeeded r)))
    (is (zero? (q/size)))
    (is (= 5 (count @log)))))

(deftest property-drain-zero-progress-backs-off
  (q/enqueue! {:op :add-entry! :id "x" :args [{}]})
  (let [r (q/drain! {:dispatch-fn (fn [_] (throw (Exception. "boom")))})]
    (is (= :all-failed (:result r)))
    (is (pos? (:failed r)))
    ;; failed op re-queued
    (is (pos? (q/size)))))

;; ---- Mutation -------------------------------------------------------------

(deftest mutation-stats-map-shape
  (let [s (q/stats)]
    (is (contains? s :enqueued))
    (is (contains? s :depth))
    (is (contains? s :max-size))
    (is (contains? s :draining?))))

(deftest mutation-coalesce-keeps-latest
  (let [ops [{:op :update-entry! :id "k" :args ["k" {:v 1}]}
             {:op :update-entry! :id "k" :args ["k" {:v 99}]}]
        out (q/coalesce ops)]
    (is (= 1 (count out)))
    (is (= 99 (-> out first :args second :v)))))

(deftest golden-coalesce-folds-partial-updates-to-one-id
  (testing "an update carries only its own fields: keeping the latest would drop the rest"
    (let [out (q/coalesce [{:op :update-entry! :id "k" :args ["k" {:tags ["a"] :v 1}]}
                           {:op :add-entry! :id "j" :args [{:id "j"}]}
                           {:op :update-entry! :id "k" :args ["k" {:duration "long" :v 2}]}])]
      (is (= [["k" {:tags ["a"] :duration "long" :v 2}] [{:id "j"}]]
             (mapv :args out))
          "one op per id, in first-seen order, the later field winning"))))

(deftest golden-coalesce-retires-an-embed-text-its-content-replaced
  (let [fold (fn [u1 u2]
               (-> (q/coalesce [{:op :update-entry! :id "k" :args ["k" u1]}
                                {:op :update-entry! :id "k" :args ["k" u2]}])
                   first :args second))]
    (is (= {:content "B"} (fold {:content "A" :embed-text "a"} {:content "B"}))
        "run in turn, the second update would embed its own :content")
    (is (= {:content "B" :embed-text "b"}
           (fold {:content "A" :embed-text "a"} {:content "B" :embed-text "b"})))
    (is (= {:content "A" :embed-text "a" :tags ["t"]}
           (fold {:content "A" :embed-text "a"} {:tags ["t"]})))))

(deftest golden-coalesce-folds-every-op-on-one-id-in-order
  (let [add    (fn [e] {:op :add-entry! :id "k" :args [(assoc e :id "k")]})
        update (fn [u] {:op :update-entry! :id "k" :args ["k" u]})
        del    {:op :delete-entry! :id "k" :args ["k"]}
        fold   (fn [& ops] (mapv #(select-keys % [:op :args]) (q/coalesce ops)))]
    (testing "an add replaces whatever was queued before it"
      (is (= [{:op :add-entry! :args [{:id "k" :v 2}]}]
             (fold (add {:v 1}) (update {:tags ["b"]}) (add {:v 2})))
          "keyed by (op, id), the update replayed after the second add")
      (is (= [{:op :add-entry! :args [{:id "k" :v 2}]}]
             (fold (update {:tags ["b"]}) (add {:v 2}))))
      (is (= [{:op :add-entry! :args [{:id "k" :v 2}]}]
             (fold del (add {:v 2})))
          "an upsert overwrites the whole point: the delete adds nothing"))
    (testing "an update folds into the add queued before it"
      (is (= [{:op :add-entry! :args [{:id "k" :v 1 :tags ["b"]}]}]
             (fold (add {:v 1 :tags ["a"]}) (update {:tags ["b"]}))))
      (is (= "k" (-> (fold (add {:v 1}) (update {:id "other"})) first :args first :id))
          "update-entry! merges {:id id} last: the id holds")
      (is (= [{:op :add-entry! :args [{:id "k" :content "B"}]}]
             (fold (add {:content "A" :embed-text "a"}) (update {:content "B"})))
          "an :embed-text whose :content was replaced is retired")
      (is (= [{:op :add-entry! :args [{:id "k" :content "A" :embed-text "a" :tags ["t"]}]}]
             (fold (add {:content "A" :embed-text "a"}) (update {:tags ["t"]})))))
    (testing "a delete replaces whatever was queued before it"
      (is (= [{:op :delete-entry! :args ["k"]}]
             (fold (add {:v 1}) (update {:v 2}) del))))
    (testing "an update after a delete is dropped: the entry it would update is gone"
      (is (= [{:op :delete-entry! :args ["k"]}]
             (fold (update {:v 1}) del (update {:v 2})))))
    (testing "an update after a delete and a re-add lands on the re-add"
      (is (= [{:op :add-entry! :args [{:id "k" :v 3 :duration "long"}]}]
             (fold (update {:tags ["u1"]}) del (add {:v 3}) (update {:duration "long"})))))))

(deftest golden-coalesce-keeps-first-seen-order-across-ids
  (let [out (q/coalesce [{:op :update-entry! :id "b" :args ["b" {:v 1}]}
                         {:op :add-entry! :id "a" :args [{:id "a"}]}
                         {:op :cleanup-expired! :args []}
                         {:op :delete-entry! :id "b" :args ["b"]}
                         {:op :add-entry! :id "c" :args [{:id "c"}]}
                         {:op :cleanup-expired! :args []}
                         {:op :update-entry! :id "a" :args ["a" {:v 2}]}])]
    (is (= [[:delete-entry! "b"] [:add-entry! "a"] [:cleanup-expired! nil] [:add-entry! "c"]]
           (mapv (juxt :op :id) out)))
    (is (= [{:id "a" :v 2}] (:args (second out))))))

(deftest a-failed-op-goes-back-ahead-of-ops-queued-while-it-ran
  (q/enqueue! {:op :add-entry! :id "k" :args [{:id "k" :v 1}]})
  (let [first? (atom true)]
    (q/drain! {:dispatch-fn (fn [_]
                              (when (compare-and-set! first? true false)
                                ;; a newer write to the same id queues mid-pass
                                (q/enqueue! {:op :update-entry! :id "k" :args ["k" {:tags ["b"]}]})
                                (throw (Exception. "boom"))))}))
  (is (= 2 (q/size)))
  (let [seen (atom [])]
    (q/drain! {:dispatch-fn #(swap! seen conj (select-keys % [:op :args]))})
    (is (= [{:op :add-entry! :args [{:id "k" :v 1 :tags ["b"]}]}] @seen)
        "queued behind the update, the failed add replayed last and dropped it")))

(deftest mutation-clear-resets-depth
  (q/enqueue! {:op :add-entry! :id "z" :args [{}]})
  (is (pos? (q/size)))
  (q/clear!)
  (is (zero? (q/size))))
