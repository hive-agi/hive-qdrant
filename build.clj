(ns build
  "hive-qdrant library build: source jar + Clojars deploy.

   Version = the repo VERSION file (falls back to 0.{minor}.{git-count}).
   Reads coordinates from ./version.edn. `:jar-excludes` is a coll of
   class-dir-relative paths deleted after copy, before jarring — used to
   keep the host-integration namespaces (addon, lifecycle) and the addon
   manifest OUT of the published core so it stays Maven-clean. Those are
   the only nses that require hive-mcp, which is not on Clojars.

   Tasks (invoke with `clojure -T:build <task>`):
     jar      build the source jar under target/
     install  jar + install to the local ~/.m2 (no network)
     deploy   jar + push to Clojars (needs CLOJARS_USERNAME / CLOJARS_PASSWORD)"
  (:require [clojure.tools.build.api :as b]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [deps-deploy.deps-deploy :as dd]))

(def ^:private cfg (edn/read-string (slurp "version.edn")))
(def lib (:lib cfg))
(def version
  (let [f (io/file "VERSION")]
    (if (.exists f)
      (str/trim (slurp f))
      (format "0.%s.%s" (:minor cfg 0) (b/git-count-revs nil)))))
(def ^:private class-dir "target/classes")
(def ^:private src-dirs (:src-dirs cfg ["src"]))
(def ^:private jar-excludes (:jar-excludes cfg []))
(def ^:private pom-exclude-deps (set (:pom-exclude-deps cfg [])))
(def ^:private jar-file (format "target/%s-%s.jar" (name lib) version))

(defn- basis
  "Project basis for the POM, minus `:pom-exclude-deps` (integration-only libs
   whose namespaces are jar-excluded)."
  []
  (let [proj (edn/read-string (slurp "deps.edn"))
        core (apply dissoc (:deps proj) pom-exclude-deps)]
    (b/create-basis {:project (assoc proj :deps core)})))

(defn- write-pom []
  (b/write-pom
   {:class-dir class-dir
    :lib       lib
    :version   version
    :basis     (basis)
    :src-dirs  (vec (remove #{"resources"} src-dirs))
    :scm       {:url (:scm-url cfg)
                :tag (b/git-process {:git-args "rev-parse HEAD"})}
    :pom-data  [[:licenses
                 [:license
                  [:name (get-in cfg [:license :name] "MIT")]
                  [:url  (get-in cfg [:license :url]
                                 "https://opensource.org/licenses/MIT")]]]]}))

(defn clean [_] (b/delete {:path "target"}))

(defn jar
  "Build the source jar (pom + copied sources, minus :jar-ignores) under target/."
  [_]
  (clean nil)
  (write-pom)
  (b/copy-dir {:src-dirs src-dirs :target-dir class-dir})
  (doseq [rel jar-excludes]
    (b/delete {:path (str class-dir "/" rel)}))
  (b/jar {:class-dir class-dir :jar-file jar-file})
  (println "Built" (str lib) version "->" jar-file))

(defn install
  "Build + install to the local ~/.m2 repository (offline; for verification)."
  [_]
  (jar nil)
  (dd/deploy {:installer :local
              :artifact  jar-file
              :pom-file  (b/pom-path {:lib lib :class-dir class-dir})})
  (println "Installed" (str lib) version "to ~/.m2"))

(defn- already-published?
  []
  (let [[grp art] (str/split (str lib) #"/")
        url (format "https://repo.clojars.org/%s/%s/%s/%s-%s.jar"
                    (str/replace grp "." "/") art version art version)]
    (try
      (let [conn (doto ^java.net.HttpURLConnection (.openConnection (java.net.URL. url))
                   (.setRequestMethod "HEAD")
                   (.setConnectTimeout 10000)
                   (.setReadTimeout 10000))]
        (= 200 (.getResponseCode conn)))
      (catch Throwable _ false))))

(defn deploy
  "Build + deploy to Clojars. Requires CLOJARS_USERNAME and CLOJARS_PASSWORD
   (a deploy token). No-ops if the version is already published."
  [_]
  (if (already-published?)
    (println "Skip:" (str lib) version "already on Clojars — bump VERSION to release.")
    (do
      (jar nil)
      (dd/deploy {:installer :remote
                  :artifact  jar-file
                  :pom-file  (b/pom-path {:lib lib :class-dir class-dir})})
      (println "Deployed" (str lib) version "to Clojars"))))
