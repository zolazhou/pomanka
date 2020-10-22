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


(defrecord Producer [config database]
  component/Lifecycle
  (start [this]
    (let [topics (q.topics/load-all database)]
      (assoc this :topics (atom topics))))
  (stop [this]
    this))

(defn new-producer
  [config]
  (map->Producer {:config config}))

(s/def ::producer (partial instance? Producer))

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

;; TODO async producer
(defn send!
  [{:keys [database] :as context}
   {:keys [topic key payload]}]
  (let [{:topic/keys [name partitions]} (get-topic! context topic)
        table (str name "_" (get-partition partitions key))
        sql   (str "INSERT INTO " table " (payload) VALUES (?)")]
    (db/execute! database [sql (if (string? payload)
                                 (.getBytes payload "UTF-8")
                                 payload)])))
