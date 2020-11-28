(ns pomanka.queue.messages
  (:require
    [next.jdbc.result-set :as rs]
    [pomanka.database :as db]
    [taoensso.truss :refer [have]]))


(defn load-messages
  [database topic partition offset limit]
  (let [sql (str "SELECT * FROM topics." topic "_" (have nat-int? partition)
                 " WHERE id > ? ORDER BY id LIMIT ?")]
    (db/execute! database
                 [sql (or (have [:or nil? nat-int?] offset) 0) (have nat-int? limit)]
                 {:builder-fn rs/as-unqualified-maps})))
