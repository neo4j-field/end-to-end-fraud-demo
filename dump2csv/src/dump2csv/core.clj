(ns dump2csv.core
  (:import [org.neo4j.driver Driver GraphDatabase AuthTokens Values])
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(defn -to-pojos
  [list]
  (map #(.asObject %) list))

(defn query
  [^Driver driver ^String cypher]
  (with-open [session (.session driver)]
    (let [result (.run session cypher)
          values (->> (iterator-seq result)
                      (map #(.values %))
                      (map -to-pojos))
          writer (io/writer *out*)]
      (do
        (csv/write-csv writer values)
        ;; Need a manual flush since it's stdout
        (.flush writer)))))

(def uri "neo4j+s://1179cd74.databases.neo4j.io:7687")
(def username "neo4j")
(def password "password")
(def auth (AuthTokens/basic username password))

(defn -log
  [& args]
  (.println *err* args))

(defn -main
  [& args]
  (with-open [driver (GraphDatabase/driver uri auth)]
    (let [cypher (first args)]
      (do
        (-log "cypher: " cypher)
        (query driver (or (first args) "UNWIND range(1, 10) AS x RETURN x;"))))))
