(ns pomanka.system
  (:require
    [calyx.system :refer [system-map]]
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [com.fulcrologic.guardrails.core :refer [>defn =>]]
    [com.stuartsierra.component :as component]
    [nrepl.server :as nrepl]
    [pomanka.bottlewater.dumper :refer [new-dumper]]
    [pomanka.config :as config]
    [pomanka.database :as db]
    [pomanka.queue.broker :refer [new-broker]]
    [pomanka.queue.producer :refer [new-producer]]))


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

(s/def ::database ::db/database)
(s/def ::nrepl (partial instance? NReplServer))
(s/def ::system (s/keys :opt-un [::database
                                 ::nrepl]))

;; system

(>defn app-system
  [config]
  [::config/config => ::system]
  (let [system (cond-> {}
                 ;; database
                 (:database config)
                 (assoc :database (db/new-database (:database config)))

                 ;; producer
                 (:producer config)
                 (assoc :producer (component/using
                                    (new-producer (:producer config))
                                    [:database]))
                 ;; dumper
                 (:dumper config)
                 (assoc :dumper (component/using
                                  (new-dumper (:dumper config))
                                  [:database :producer]))

                 ;; broker
                 (:broker config)
                 (assoc :broker (component/using
                                  (new-broker (:broker config))
                                  [:database]))

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

