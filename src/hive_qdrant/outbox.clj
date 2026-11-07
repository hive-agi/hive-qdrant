(ns hive-qdrant.outbox
  "Durable spill file for the replay queue: one EDN op per line.

   The queue (hive-qdrant.queue) stays the in-memory owner of the ops; an
   outbox is the on-disk mirror it appends to before it acknowledges a
   queued write, and rewrites after a drain pass. On restart `load-ops`
   reads the file back so the queue starts where the last process stopped.

   Layers:
     pure     `durable-op`, `encode-op`, `decode-lines`
     boundary `FileOutbox` (`append!`, `rewrite!`, `load-ops`)

   A line that does not read back as an op map is counted and skipped, never
   fatal: a torn last line from a crash must not cost the lines before it."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log])
  (:import [java.io File]
           [java.nio.charset StandardCharsets]
           [java.nio.file CopyOption Files StandardCopyOption]
[java.nio.file OpenOption]
[java.nio.file StandardOpenOption]))

;; =============================================================================
;; Port
;; =============================================================================

(defprotocol IOutbox
  (append! [this op]
    "Durably record OP. Returns true once the bytes are synced, else false.")
  (rewrite! [this ops]
    "Replace the whole record with OPS, atomically. Returns true or false.")
  (load-ops [this]
    "Every op recorded, in order: {:ops [..] :corrupt n}."))

;; =============================================================================
;; Pure
;; =============================================================================

(defn- drop-embed-text
  [m]
  (if (map? m) (dissoc m :embed-text "embed-text") m))

(defn durable-op
  "OP as it is written to disk: without the transient :embed-text of the
   entry or updates it carries. That text can be the plaintext of a sealed
   :content, and the disk record must never hold it; a replay from disk
   embeds the stored :content instead."
  [op]
  (update op :args (fn [args] (some->> args (mapv drop-embed-text)))))

(defn encode-op
  "One outbox line for OP (no trailing newline)."
  [op]
  (binding [*print-length* nil
            *print-level*  nil
            *print-meta*   false]
    (pr-str (durable-op op))))

(defn- read-line-op
  [line]
  (try
    (let [v (edn/read-string line)]
      (when (and (map? v) (keyword? (:op v))) v))
    (catch Exception _ nil)))

(defn decode-lines
  "Ops from outbox LINES, in order, blank lines ignored:
   {:ops [..] :corrupt n} where n counts the lines that were not ops."
  [lines]
  (reduce (fn [acc line]
            (if (str/blank? line)
              acc
              (if-let [op (read-line-op line)]
                (update acc :ops conj op)
                (update acc :corrupt inc))))
          {:ops [] :corrupt 0}
          lines))

;; =============================================================================
;; Boundary
;; =============================================================================

(defn- write-synced!
  "Write TEXT to F with SYNC: the call returns only once content and
   metadata are on the device. APPEND? appends, else truncates."
  [^File f ^String text append?]
  (Files/write (.toPath f)
               (.getBytes text StandardCharsets/UTF_8)
               ^"[Ljava.nio.file.OpenOption;"
               (into-array OpenOption
                           (cond-> [StandardOpenOption/CREATE
                                    StandardOpenOption/WRITE
                                    StandardOpenOption/SYNC]
                             append?       (conj StandardOpenOption/APPEND)
                             (not append?) (conj StandardOpenOption/TRUNCATE_EXISTING)))))

(defrecord FileOutbox [^File file lock]
  IOutbox
  (append! [_ op]
    (try
      (locking lock
        (io/make-parents file)
        (write-synced! file (str (encode-op op) "\n") true))
      true
      (catch Throwable t
        (log/warn "hive-qdrant.outbox append failed:" (str file) (ex-message t))
        false)))

  (rewrite! [_ ops]
    (try
      (locking lock
        (io/make-parents file)
        (let [tmp (File. (str (.getPath file) ".tmp"))]
          (write-synced! tmp (apply str (map #(str (encode-op %) "\n") ops)) false)
          (Files/move (.toPath tmp) (.toPath file)
                      (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE
                                              StandardCopyOption/REPLACE_EXISTING]))))
      true
      (catch Throwable t
        (log/warn "hive-qdrant.outbox rewrite failed:" (str file) (ex-message t))
        false)))

  (load-ops [_]
    (locking lock
      (if (.exists file)
        (let [r (decode-lines (str/split-lines (slurp file :encoding "UTF-8")))]
          (when (pos? (:corrupt r))
            (log/warn "hive-qdrant.outbox skipped" (:corrupt r)
                      "unreadable line(s) in" (str file)))
          r)
        {:ops [] :corrupt 0}))))

(defn file-outbox
  "An outbox kept in the file at PATH (parents created on first write)."
  [path]
  (->FileOutbox (io/file path) (Object.)))

(defn default-path
  "Where the outbox lives when no path is configured:
   $XDG_DATA_HOME/hive/qdrant/outbox.edn, else ~/.local/share/hive/qdrant/outbox.edn."
  []
  (let [xdg  (System/getenv "XDG_DATA_HOME")
        base (if (str/blank? xdg)
               (str (System/getProperty "user.home") "/.local/share")
               xdg)]
    (str base "/hive/qdrant/outbox.edn")))
