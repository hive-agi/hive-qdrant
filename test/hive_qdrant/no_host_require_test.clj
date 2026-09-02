(ns hive-qdrant.no-host-require-test
  "hive-mcp is the HOST, and it is not published to maven. A load-time
   `(:require [hive-mcp...])` anywhere under src/ makes this jar unloadable
   wherever the host is absent, and the failure surfaces as a
   FileNotFoundException at the consumer's require, far from the line that
   caused it. What the addon needs from the host is soft-resolved through the
   var at call time (hive-addon.host)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is]]))

(defn- clj-sources []
  (->> (file-seq (io/file "src"))
       (filter #(.isFile ^java.io.File %))
       (filter #(re-find #"\.cljc?$" (.getName ^java.io.File %)))))

(defn- ns-form
  "Read the first form of `file`, which is its ns form, or nil if unreadable."
  [^java.io.File file]
  (try
    (with-open [r (java.io.PushbackReader. (io/reader file))]
      (read {:read-cond :allow :eof nil} r))
    (catch Exception _ nil)))

(defn- host-requires
  "Namespaces starting with hive-mcp that `ns-form` requires."
  [form]
  (->> (tree-seq coll? seq form)
       (filter symbol?)
       (map str)
       (filter #(str/starts-with? % "hive-mcp."))
       set))

(deftest no-src-namespace-requires-the-host-test
  (let [offenders (->> (clj-sources)
                       (keep (fn [f]
                               (when-let [hits (seq (host-requires (ns-form f)))]
                                 [(.getPath ^java.io.File f) (vec hits)])))
                       (into {}))]
    (is (seq (clj-sources)) "sources were found, so an empty result means something")
    (is (= {} offenders)
        (str "these src namespaces require the host at load time: " offenders))))
