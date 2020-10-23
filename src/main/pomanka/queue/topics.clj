(ns pomanka.queue.topics
  (:require
    [com.fulcrologic.guardrails.core :refer [>defn >defn- ? =>]]
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

(>defn load-all
  [database]
  [::db/executable => map?]
  (let [data (db/execute! database {:select [:*]
                                    :from   [:topics]})]
    (->> data
         (map (fn [topic]
                [(:topic/name topic) topic]))
         (into {}))))
