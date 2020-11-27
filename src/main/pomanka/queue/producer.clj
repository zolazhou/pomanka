(ns pomanka.queue.producer
  (:require
    [clojure.spec.alpha :as s]
    [com.stuartsierra.component :as component]
    [crypto.random :as random]
    [pomanka.queue.topics :as q.topics]
    [pomanka.database :as db]))


(s/def ::topic string?)
(s/def ::key string?)
(s/def ::payload string?)
(s/def ::message (s/keys :req-un [::topic ::payload]
                         :opt-un [::key]))

(defn- get-topic!
  [{:keys [database topics]} topic-name]
  (if-some [topic (get @topics topic-name)]
    topic
    (let [topic (q.topics/create-topic! database
                                        #:topic{:name       topic-name
                                                :partitions 2})]
      (swap! topics assoc topic-name topic)
      topic)))

(defn- get-partition
  [n k]
  (mod (hash (or k (random/bytes 10))) n))

(defn ensure-topic!
  [producer topic-name]
  (get-topic! producer topic-name))

;; TODO async producer
(defn send!
  [{:keys [database] :as producer}
   {:keys [topic key payload]}]
  (let [{:topic/keys [name partitions]} (get-topic! producer topic)
        table (str "topics." name "_" (get-partition partitions key))
        sql   (str "INSERT INTO " table " (payload) VALUES (?)")]
    (db/execute! database [sql (if (string? payload)
                                 (.getBytes payload "UTF-8")
                                 payload)])))

(defn create-producer
  [database]
  {:database database
   :topics   (atom (q.topics/load-all database))})

(defrecord Producer [config database]
  component/Lifecycle
  (start [this]
    (assoc this :producer (create-producer database)))
  (stop [this]
    this))

(defn new-producer
  [config]
  (map->Producer {:config config}))

(s/def ::producer (partial instance? Producer))
