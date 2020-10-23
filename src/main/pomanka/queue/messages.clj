(ns pomanka.queue.messages
  (:require
    [next.jdbc.result-set :as rs]
    [pomanka.database :as db]))


(defn load-messages
  [database topic partition offset limit]
  (let [sql (str "SELECT * FROM topics." topic "_" partition
                 " WHERE id > ? LIMIT ?")]
    (db/execute! database
                 [sql (or offset 0) limit]
                 {:builder-fn rs/as-unqualified-maps})))
