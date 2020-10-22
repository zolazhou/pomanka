(ns pomanka.client.core
  (:require
    [aleph.tcp :as tcp]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [pomanka.protocol :as protocol])
  (:import [java.io Closeable]))


(defn connect
  [host port]
  (d/chain
    (tcp/client {:host host, :port port})
    #(protocol/wrap-duplex-stream %)))

(defn subscribe
  [client consumer topics]
  (d/chain
    (s/put! client {:type     :subscribe
                    :consumer consumer
                    :topics   topics})
    (fn [_] (s/take! client))))

(defn fetch
  [client consumers]
  (d/chain
    (s/put! client {:type      :fetch
                    :consumers consumers})
    (fn [_] (s/take! client))))

(defn commit
  [client offsets]
  (d/chain
    (s/put! client {:type    :commit
                    :offsets offsets})
    (fn [_] (s/take! client))))

(defn close
  [^Closeable client]
  (.close client))

(defn group-by-partition
  [messages]
  (->> messages
       (group-by :topic)
       (map (fn [[topic msgs]]
              [topic (group-by :partition msgs)]))
       (into {})))
