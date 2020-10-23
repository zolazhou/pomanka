(ns pomanka.dumper.core
  (:require
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [>defn >defn- =>]]
    [pomanka.database :as db]
    [clojure.tools.logging :as log])
  (:import
    [io.debezium.engine DebeziumEngine
                        DebeziumEngine$Builder
                        DebeziumEngine$ChangeConsumer
                        DebeziumEngine$RecordCommitter]
    [java.io Closeable]
    [java.util Properties List]
    [java.util.concurrent Executors ExecutorService TimeUnit]
    [org.apache.kafka.connect.errors ConnectException]))


(set! *warn-on-reflection* true)

(s/def ::source ::db/config)
(s/def ::name string?)
(s/def ::connector string?)
(s/def ::slot-name string?)
(s/def ::publication string?)
(s/def ::storage string?)
(s/def ::props map?)
(s/def ::flush-interval pos-int?)
(s/def ::offset (s/keys :opt-un [::storage
                                 ::props
                                 ::flush-interval]))
(s/def ::config (s/keys :req-un [::source
                                 ::offset
                                 ::slot-name
                                 ::publication]
                        :opt-un [::name
                                 ::connector]))

(defn- offset-config
  [{:keys [storage flush-interval props]
    :or   {storage        "pomanka.dumper.offset.PostgresOffsetBackingStore"
           flush-interval 10000}}]
  (assoc
    (->> props
         (map (fn [[k v]] [(str "offset.storage." k) v]))
         (into {}))
    "offset.storage" storage
    "offset.flush.interval.ms" flush-interval))

(defn- engine-config
  [{:keys [source offset name server-name slot-name publication connector extra]
    :or   {name      "engine"
           connector "io.debezium.connector.postgresql.PostgresConnector"}}]
  (let [config (merge {"name"                 name
                       "connector.class"      connector
                       "plugin.name"          "pgoutput"
                       "slot.name"            slot-name
                       "publication.name"     publication
                       "database.server.name" server-name
                       "database.hostname"    (:hostname source)
                       "database.port"        (:port source)
                       "database.user"        (:username source)
                       "database.password"    (:password source)
                       "database.dbname"      (:dbname source)}
                      (offset-config offset)
                      extra)
        props  (Properties.)]
    (doseq [[k v] config]
      (.setProperty props k (str v)))
    props))

(>defn create-dumper
  [config handler]
  [::config fn? => any?]
  (let [props   (engine-config config)
        builder (DebeziumEngine/create
                  (Class/forName "io.debezium.engine.format.Json"))
        engine  (-> ^DebeziumEngine$Builder builder
                    (.using ^Properties props)
                    (.notifying
                      (reify DebeziumEngine$ChangeConsumer
                        (^void handleBatch [_this
                                            ^List records
                                            ^DebeziumEngine$RecordCommitter committer]
                          (handler records committer))))
                    (.build))]
    {::engine   engine
     ::executor (Executors/newSingleThreadExecutor)}))

(defn start
  [{::keys [engine ^ExecutorService executor]}]
  (.execute executor engine))

(defn- shutdown-executor
  [^ExecutorService executor]
  (.shutdown executor)
  (try
    (.awaitTermination executor 30 TimeUnit/SECONDS)
    (catch InterruptedException _ex
      (.interrupt (Thread/currentThread))))
  (when-not (.isEmpty (.shutdownNow executor))
    (throw (ConnectException.
             (str "Failed to stop Postgres Dumper. Exiting without "
                  "cleanly shutting down pending tasks and/or callbacks.")))))

(defn stop
  [{::keys [engine ^ExecutorService executor]}]
  (.close ^Closeable engine)
  (shutdown-executor executor))
