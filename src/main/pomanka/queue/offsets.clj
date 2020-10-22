(ns pomanka.queue.offsets
  (:require
    [next.jdbc :as jdbc]
    [next.jdbc.prepare :as prep]
    [pomanka.database :as db]))


(def save-sql
  (str "INSERT INTO offsets (consumer, topic, partition, value) "
       "VALUES (?, ?, ?, ?) "
       "ON CONFLICT (consumer, topic, partition) "
       "DO UPDATE SET value = ?"))

(defn save-all
  [database offsets]
  (db/with-transaction [tx database]
    (with-open [ps (jdbc/prepare tx [save-sql])]
      (prep/execute-batch!
        ps
        (mapv (fn [[consumer topic partition offset]]
                [consumer topic partition offset offset])
              offsets)))))

(defn load-all
  [database]
  (let [offsets (db/execute! database {:select [:*]
                                       :from   [:offsets]})]
    (reduce
      (fn [acc {:offset/keys [consumer topic partition value]}]
        (assoc acc [consumer topic partition] value))
      {}
      offsets)))
