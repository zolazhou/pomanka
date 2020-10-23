(ns pomanka.config
  (:require
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [cprop.core]
    [cprop.source]
    [pomanka.bottlewater.core :as bottlewater]
    [pomanka.queue.broker :as broker]))


(s/def ::bottlewater ::bottlewater/config)
(s/def ::broker ::broker/config)

;; config
(s/def ::config (s/keys :req-un [::bottlewater ::broker]
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
