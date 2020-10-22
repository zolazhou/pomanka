(ns pomanka.config
  (:require
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [cprop.core]
    [cprop.source]))


;; database
(s/def :database/hostname string?)
(s/def :database/port pos-int?)
(s/def :database/dbname string?)
(s/def :database/username string?)
(s/def :database/password string?)
(s/def :database/driver string?)
(s/def :database/pool-size pos-int?)
(s/def ::database (s/keys :req-un [:database/hostname
                                   :database/port
                                   :database/dbname
                                   :database/username
                                   :database/password]
                          :opt-un [:database/driver
                                   :database/pool-size]))

;; dumper
(s/def :dumper/source ::database)
(s/def :dumper/database ::database)
(s/def :dumper/name string?)
(s/def :dumper/slot-name string?)
(s/def :dumper/publication string?)
(s/def :dumper/topic-name string?)
(s/def ::dumper (s/keys :req-un [:dumper/source
                                 :dumper/database
                                 :dumper/slot-name
                                 :dumper/publication
                                 :dumper/topic-name]
                        :opt-un [:dumper/name]))

;; broker
(s/def :broker/port pos-int?)
(s/def :broker/offsets-save-interval pos-int?)
(s/def ::broker (s/keys :req-un [:broker/port
                                 :broker/offsets-save-interval]))

;; config
(s/def ::config (s/keys :req-un [::dumper ::database ::broker]
                        :opt-un []))


(defn load-config
  ([]
   (load-config nil))
  ([file]
   (let [env      (cprop.source/from-env)
         file     (or file (:pomanka-config-file env))
         resource (:pomanka-config-resource env)]
     (log/info "Load config from file:" file ", resource:" resource)
     (let [value (if (some? file)
                   (cprop.core/load-config :file file)
                   (cprop.core/load-config :resource resource))]
       (if (s/invalid? (s/conform ::config value))
         (throw (ex-info "Invalid configuration"
                         {:explain (s/explain-data ::config value)}))
         value)))))
