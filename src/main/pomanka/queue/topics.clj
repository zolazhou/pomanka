(ns pomanka.queue.topics
  (:require
    [honeysql.helpers :as h]
    [pomanka.database :as db]))


(defn- create-record-table
  [database name]
  (db/execute!
    database
    [(str "CREATE TABLE IF NOT EXISTS topics." name "(\n"
          "  id BIGSERIAL CONSTRAINT " name "_pk PRIMARY KEY,\n"
          "  ts TIMESTAMP DEFAULT NOW() NOT NULL,\n"
          "  payload BYTEA\n"
          ")")]))

(defn create-topic!
  [database {:topic/keys [name partitions] :as topic}]
  (db/with-transaction [tx database]
    (doseq [n (range partitions)]
      (create-record-table tx (str name "_" n)))
    (db/execute-one! tx
                     (-> (h/insert-into :topics)
                         (h/values [topic]))
                     {:return-keys true})))

(defn load-all
  [database]
  (let [data (db/execute! database {:select [:*]
                                    :from   [:topics]})]
    (->> data
         (map (fn [topic]
                [(:topic/name topic) topic]))
         (into {}))))

(defn get-topic
  [database topic-name]
  (db/execute-one!
    database
    {:select [:*]
     :from   [:topics]
     :where  [:= :name topic-name]}))

(defn get-or-create-topic!
  [database topic-name]
  (or (get-topic database topic-name)
      (create-topic! database #:topic{:name       topic-name
                                      :partitions 2})))
