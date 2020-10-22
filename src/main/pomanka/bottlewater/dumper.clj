(ns pomanka.bottlewater.dumper
  (:require
    [calyx.json :as json]
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [>defn >defn- =>]]
    [com.stuartsierra.component :as component]
    [pomanka.bottlewater.schema :as schemas]
    [pomanka.database :as db]
    [pomanka.queue.producer :as producer]
    [pomanka.queue.topics :as q.topics])
  (:import
    [io.debezium.engine ChangeEvent
                        DebeziumEngine
                        DebeziumEngine$Builder
                        DebeziumEngine$ChangeConsumer
                        DebeziumEngine$RecordCommitter]
    [java.io Closeable]
    [java.util Properties List]
    [java.util.concurrent Executors ExecutorService TimeUnit]
    [org.apache.kafka.connect.errors ConnectException]))


(set! *warn-on-reflection* true)

(s/def ::source ::db/config)
(s/def ::database ::db/config)
(s/def ::name string?)
(s/def ::slot-name string?)
(s/def ::publication string?)
(s/def ::topic-name string?)
(s/def ::config (s/keys :req-un [::source
                                 ::database
                                 ::slot-name
                                 ::publication
                                 ::topic-name]
                        :opt-un [::name]))

(defn- engine-config
  [{:keys [source database name server-name slot-name publication]}]
  (let [config {"name"                             (or name "engine")
                "offset.storage"                   "pomanka.bottlewater.offset.PostgresOffsetBackingStore"
                "offset.storage.postgres.hostname" (:hostname database)
                "offset.storage.postgres.port"     (:port database)
                "offset.storage.postgres.username" (:username database)
                "offset.storage.postgres.password" (:password database)
                "offset.storage.postgres.dbname"   (:dbname database)
                "offset.flush.interval.ms"         "10000"
                "connector.class"                  "io.debezium.connector.postgresql.PostgresConnector"
                "plugin.name"                      "pgoutput"
                "slot.name"                        slot-name
                "publication.name"                 publication
                ;;"database.history"                 "io.debezium.relational.history.FileDatabaseHistory"
                ;;"database.history.file.filename"   "/tmp/db-history.dat"
                "database.server.name"             server-name
                "database.hostname"                (:hostname source)
                "database.port"                    (:port source)
                "database.user"                    (:username source)
                "database.password"                (:password source)
                "database.dbname"                  (:dbname source)}
        props  (Properties.)]
    (doseq [[k v] config]
      (.setProperty props k (str v)))
    props))

(defn- schema->id
  [{:keys [datasource schemas]} schema]
  (let [json   (json/encode schema)
        sha    (schemas/schema-sha json)
        schema (if-some [schema (get @schemas sha)]
                 schema
                 (let [schema (schemas/new-schema! datasource json)]
                   (swap! schemas assoc sha schema)
                   schema))]
    (:bw_schema/id schema)))

(defn- compress-payload
  "Removed unchanged columns from before field"
  [{:keys [op before after] :as payload}]
  (case op
    "u" (reduce
          (fn [acc [k v]]
            (if (not= v (get after k))
              (assoc acc k v)
              acc))
          {}
          before)
    payload))

(defn- convert-record
  "Convert schema to it's id"
  [context ^ChangeEvent record]
  (let [key     (json/decode (.key record))
        value   (json/decode (.value record))
        convert (partial schema->id context)]
    {:key   (update key :schema convert)
     :value (-> value
                (update :schema convert)
                (update :payload compress-payload))}))

(defn- handle-events
  [{:keys [topic-name] :as context}
   ^List records
   ^DebeziumEngine$RecordCommitter committer]
  (doseq [record records]
    (producer/send!
      context
      {:topic   topic-name
       :payload (json/encode (convert-record context record))})
    (.markProcessed committer record)))

(>defn- create-engine
  [config database]
  [::config ::db/datasource => any?]
  (let [props   (engine-config config)
        builder (DebeziumEngine/create
                  (Class/forName "io.debezium.engine.format.Json"))
        context {:topic-name (:topic-name config)
                 :database   database
                 :topics     (atom (q.topics/load-all database))
                 :schemas    (atom (schemas/load-all database))}]
    (-> ^DebeziumEngine$Builder builder
        (.using ^Properties props)
        (.notifying
          (reify DebeziumEngine$ChangeConsumer
            (^void handleBatch [_this
                                ^List records
                                ^DebeziumEngine$RecordCommitter committer]
              (handle-events context records committer))))
        (.build))))

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

(defrecord Dumper [config]
  component/Lifecycle
  (start [this]
    (let [database (db/create-datasource (:database config))
          engine   (create-engine config database)
          executor (Executors/newSingleThreadExecutor)]
      (.execute executor engine)
      (assoc this :database database
                  :engine engine
                  :executor executor)))
  (stop [{:keys [database engine executor]
          :as   this}]
    (when (some? engine)
      (.close ^Closeable engine))
    (when (some? executor)
      (shutdown-executor executor))
    (when (some? database)
      (db/close-datasource database))
    (dissoc this :database :engine :executor)))

(s/def ::dumper (partial instance? Dumper))

(>defn new-dumper
  [config]
  [::config => ::dumper]
  (map->Dumper {:config config}))
