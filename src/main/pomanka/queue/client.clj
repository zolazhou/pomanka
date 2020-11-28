(ns pomanka.queue.client
  (:require
    [aleph.tcp :as tcp]
    [manifold.deferred :as d]
    [manifold.stream :as s]
    [manifold.stream :as stream]
    [pomanka.queue.protocol :as protocol]))


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
  ([client consumers]
   (fetch client consumers 0))
  ([client consumers timeout]
   (d/chain
     (s/put! client {:type      :fetch
                     :consumers (if (string? consumers)
                                  [consumers]
                                  consumers)
                     :timeout   timeout})
     (fn [_] (s/take! client)))))

(defn commit
  [client offsets]
  (d/chain
    (s/put! client {:type    :commit
                    :offsets (vec offsets)})
    (fn [_] (s/take! client))))

(defn close
  [client]
  (stream/close! client))

(defn group-by-partition
  [messages]
  (->> messages
       (group-by :topic)
       (map (fn [[topic msgs]]
              [topic (group-by :partition msgs)]))
       (into {})))
