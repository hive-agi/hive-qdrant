(ns hive-qdrant.circuit-test
  "Trifecta for circuit breaker: golden + property + mutation."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-qdrant.circuit :as c]))

(use-fixtures :each (fn [f] (c/force-reset!) (f) (c/force-reset!)))

;; ---- Golden ---------------------------------------------------------------

(deftest golden-starts-closed
  (is (c/closed?))
  (is (nil? (c/check))))

(deftest golden-trip-open
  (c/configure! {:threshold 3 :cooldown-ms 50})
  (dotimes [_ 3] (c/record-failure!))
  (is (c/open?))
  (let [r (c/check)]
    (is (false? (:success? r)))
    (is (= :circuit-open (:error r)))
    (is (pos? (:retry-after r)))))

(deftest golden-halfopen-then-closed
  (c/configure! {:threshold 2 :cooldown-ms 20})
  (dotimes [_ 2] (c/record-failure!))
  (is (c/open?))
  (Thread/sleep 30)
  (is (nil? (c/check)))                 ; probe allowed → half-open
  (is (c/half-open?))
  (c/record-success!)
  (is (c/closed?)))

;; ---- Property -------------------------------------------------------------

(deftest property-listeners-fire-on-close
  (let [calls (atom 0)
        f     (fn [] (swap! calls inc))]
    (c/subscribe! f)
    (c/configure! {:threshold 1 :cooldown-ms 10})
    (c/record-failure!)                 ; open
    (Thread/sleep 15)
    (c/check)                           ; → half-open
    (c/record-success!)                 ; → closed + notify
    (is (= 1 @calls))
    (c/unsubscribe! f)))

(deftest property-record-failure-halfopen-reopens
  (c/configure! {:threshold 1 :cooldown-ms 10})
  (c/record-failure!)
  (Thread/sleep 15)
  (c/check)                             ; → half-open
  (is (c/half-open?))
  (c/record-failure!)                   ; probe fail → reopen
  (is (c/open?)))

;; ---- Mutation -------------------------------------------------------------

(deftest mutation-idempotent-configure
  (c/configure! {})
  (is (map? (c/state))))

(deftest mutation-force-reset-clears
  (c/configure! {:threshold 1})
  (c/record-failure!)
  (is (c/open?))
  (c/force-reset!)
  (is (c/closed?)))

(deftest mutation-success-from-open-closes
  (c/configure! {:threshold 1})
  (c/record-failure!)
  (c/record-success!)
  (is (c/closed?)))
