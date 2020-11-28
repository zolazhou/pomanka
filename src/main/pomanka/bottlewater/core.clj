(ns pomanka.bottlewater.core
  (:require
    [calyx.json :as json]
    [clojure.spec.alpha :as s]
    [com.fulcrologic.guardrails.core :refer [>defn >defn- =>]]
    [com.stuartsierra.component :as component]
    [pomanka.bottlewater.schema :as schemas]
    [pomanka.dumper.core :as dumper]
    [pomanka.database :as db]
    [pomanka.queue.producer :as producer])
  (:import
    [io.debezium.engine ChangeEvent DebeziumEngine$RecordCommitter]
    [java.util List]))


(set! *warn-on-reflection* true)

(s/def ::topic string?)
(s/def ::dumper ::dumper/config)
(s/def ::target ::db/config)
(s/def ::config (s/keys :req-un [::dumper
                                 ::target
                                 ::topic]))

(s/def ::database ::db/datasource)
(s/def ::schemas any?)                                      ;; atom?
(s/def ::context (s/keys :req-un [::topic
                                  ::database
                                  ::schemas]))

(>defn- schema->id
  [{:keys [database schemas]} schema]
  [::context map? => int?]
  (let [json   (json/encode schema)
        sha    (schemas/schema-sha json)
        schema (if-some [schema (get @schemas sha)]
                 schema
                 (let [schema (schemas/new-schema! database json)]
                   (swap! schemas assoc sha schema)
                   schema))]
    (:bw_schema/id schema)))

(defn- compress-payload
  "Removed unchanged columns from after field"
  [{:keys [op before after] :as payload}]
  (if (and (= op "u") (map? before))
    (let [diff (reduce
                 (fn [acc [k v]]
                   (if (not= v (get before k))
                     (conj acc k)
                     acc))
                 []
                 after)]
      (assoc payload :after (select-keys after diff)))
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
  [{:keys [producer topic] :as context}
   ^List records
   ^DebeziumEngine$RecordCommitter committer]
  (doseq [record records]
    (producer/send!
      producer
      {:topic   topic
       :payload (json/encode (convert-record context record))})
    (.markProcessed committer record)))

(>defn- make-handler
  [context]
  [::context => fn?]
  (fn [records committer]
    (handle-events context records committer)))

(defrecord Bottlewater [config]
  component/Lifecycle
  (start [this]
    (let [topic    (:topic config)
          database (db/create-datasource (:target config))
          producer (producer/create-producer database)
          _        (producer/ensure-topic! producer topic)
          context  {:topic    topic
                    :database database
                    :producer producer
                    :schemas  (atom (schemas/load-all database))}
          dumper   (dumper/create-dumper (:dumper config)
                                         (make-handler context))]
      (dumper/start dumper)
      (assoc this :database database :dumper dumper)))
  (stop [{:keys [database dumper] :as this}]
    (when (some? dumper)
      (dumper/stop dumper))
    (when (some? database)
      (db/close-datasource database))
    (dissoc this :database :dumper)))

(s/def ::bottlewater (partial instance? Bottlewater))

(>defn new-bottlewater
  [config]
  [::config => ::bottlewater]
  (map->Bottlewater {:config config}))
