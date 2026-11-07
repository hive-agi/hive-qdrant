(ns bb-queue-tests
  "Run the queue + outbox tests under babashka (no JVM): the namespaces they
   load are pure Clojure plus core.async, which bb ships.

   bb -cp src:test:<hive-spi jar>:<hive-test jar>:<test.check jar> dev/bb_queue_tests.clj"
  (:require [clojure.test :as t]))

(def test-nses
  '[hive-qdrant.queue-test
    hive-qdrant.outbox-test])

(apply require test-nses)

(let [r (apply t/run-tests test-nses)]
  (prn (select-keys r [:test :pass :fail :error]))
  (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))
