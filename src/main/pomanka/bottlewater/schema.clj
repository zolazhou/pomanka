(ns pomanka.bottlewater.schema
  (:require
    [com.fulcrologic.guardrails.core :refer [>defn >defn- =>]]
    [pomanka.database :as db]
    [calyx.util :as util]
    [calyx.json :as json]
    [honeysql.helpers :as h]))


(>defn load-all
  [database]
  [::db/database => map?]
  (let [data (db/execute! database {:select [:*]
                                    :from   [:bw_schemas]})]
    (->> data
         (map (fn [schema]
                [(:bw_schema/sha schema) schema]))
         (into {}))))

(defn schema-sha
  [schema]
  (-> (if (string? schema) schema (json/encode schema))
      (util/sha1)
      (util/bytes->hex)))

(>defn new-schema!
  [database json-schema]
  [::db/database string? => map?]
  (db/execute-one!
    database
    (-> (h/insert-into :bw_schemas)
        (h/values [{:sha    (schema-sha json-schema)
                    :schema json-schema}]))
    {:return-keys true}))
