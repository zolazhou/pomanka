(ns pomanka.system
  (:require
    [calyx.system :refer [system-map]]
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [com.fulcrologic.guardrails.core :refer [>defn =>]]
    [com.stuartsierra.component :as component]
    [nrepl.server :as nrepl]
    [pomanka.bottlewater.core :as bottlewater]
    [pomanka.config :as config]
    [pomanka.queue.broker :as broker]))


;; nREPL

(defrecord NReplServer [hostname port]
  component/Lifecycle
  (start [this]
    (log/info "Starting nREPL at" (str hostname ":" port))
    (assoc this :server (nrepl/start-server :bind hostname :port port)))
  (stop [{:keys [server] :as this}]
    (log/info "Stopping nREPL at" (str hostname ":" port))
    (when server
      (nrepl/stop-server server))
    (dissoc this :server)))

(defn new-nrepl-server [config]
  (map->NReplServer config))

;; spec

(s/def ::bottlewater ::bottlewater/bottlewater)
(s/def ::broker ::broker/broker)
(s/def ::nrepl (partial instance? NReplServer))
(s/def ::system (s/keys :opt-un [::bottlewater
                                 ::broker
                                 ::nrepl]))

;; system

(>defn app-system
  [config]
  [::config/config => ::system]
  (let [system (cond-> {}
                 ;; bottlewater
                 (:bottlewater config)
                 (assoc :bottlewater (bottlewater/new-bottlewater
                                       (:bottlewater config)))

                 ;; broker
                 (:broker config)
                 (assoc :broker (broker/new-broker (:broker config)))

                 ;; nrepl server
                 (:nrepl config)
                 (assoc :nrepl (new-nrepl-server (:nrepl config))))]
    (->> system
         seq
         flatten
         (apply system-map))))

(>defn prod-system
  [file]
  [::file => ::system]
  (-> file config/load-config app-system))
