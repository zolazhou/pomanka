(ns pomanka.queue.broker
  (:require
    [clojure.spec.alpha :as s]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]
    [manifold.stream :as stream]
    [pomanka.protocol :as protocol]
    [pomanka.queue.messages :as q.messages]
    [pomanka.queue.offsets :as q.offsets]
    [pomanka.queue.server :as server]
    [pomanka.queue.topics :as q.topics])
  (:import [java.io Closeable]))


(s/def ::port pos-int?)
(s/def ::offsets-save-interval pos-int?)
(s/def ::config (s/keys :req-un [::port
                                 ::offsets-save-interval]))

(defn- ok [] {:type :ok})

(defn- error
  [code message]
  {:type    :error
   :code    code
   :message message})

(defn start-server
  [handler port]
  (server/start-server
    (fn [s info]
      (handler (protocol/wrap-duplex-stream s) info))
    {:port port}))

(defmulti handle (fn [_ packet] (:type packet)))

(defn- disconnect
  [{:keys [clients client-info] :as ctx}]
  (d/chain
    (d/future (handle ctx {:type :unsubscribe}))
    (fn [_]
      (dosync
        (alter clients dissoc (:remote-addr client-info))))))

(defn connection
  [ctx]
  (fn [s info]
    (log/info "Client connected: " info)
    (let [ctx (assoc ctx :client-info info)]
      (d/loop []
        ;; take a packet, and define a default value that tells us if the connection is closed
        (-> (stream/take! s ::none)
            (d/chain
              ;; first, check if there even was a packet, and then transform it on another thread
              (fn [packet]
                (if (= ::none packet)
                  ::none
                  (d/future (handle ctx packet))))
              ;; once the transformation is complete, write it back to the client
              (fn [response]
                (when (not= ::none response)
                  (stream/put! s response)))
              ;; if we were successful in our response, recur and repeat
              (fn [result]
                (if result
                  (d/recur)
                  (do
                    (disconnect ctx)
                    (log/info "Closing connection:" info)))))
            ;; if there were any issues on the far end, send a stringified exception back
            ;; and close the connection
            (d/catch
              (fn [ex]
                (log/error "Exception occurred:" ex)
                (stream/put! s (str "ERROR: " ex))
                (log/info "Closing connection:" info)
                (stream/close! s)
                (disconnect ctx))))))))

(defn- rebalance-topic
  [layout partitions op client]
  (let [clients (cond-> (set (keys layout))
                  (= op :add) (conj client)
                  (= op :remove) (disj client))]
    (if (seq clients)
      (zipmap
        clients
        (partition-all (/ partitions (count clients)) (range partitions)))
      {})))

(defn- rebalance
  [group topics op client]
  (reduce
    (fn [acc [name n]]
      (let [layout (get acc name {})]
        (assoc acc name (rebalance-topic layout n op client))))
    group
    topics))

(defn- topic-partitions
  [database topics]
  (let [all (q.topics/load-all database)]
    (->> (set topics)
         (map (fn [t] [t (:topic/partitions (get all t))]))
         (into {}))))

(defmethod handle :subscribe
  [{:keys [database consumers clients client-info]}
   {:keys [consumer topics]}]
  (log/info "Subscribe " (:remote-addr client-info) " consumer: " consumer " topics: " topics)
  (let [client (:remote-addr client-info)
        topics (topic-partitions database topics)]
    (dosync
      (alter consumers update consumer rebalance topics :add client)
      (alter clients update client assoc consumer (set (keys topics))))
    (ok)))

(defmethod handle :unsubscribe
  [{:keys [database consumers clients client-info]} packet]
  (let [client (:remote-addr client-info)]
    (log/info "Unsubscribe " (:remote-addr client-info))
    (dosync
      (when-let [subs (get @clients client)]
        (let [subs (if (seq (:consumers packet))
                     (->> (:consumers packet)
                          (map (fn [g] [g (get subs g)]))
                          (into {}))
                     subs)]
          (doseq [[consumer topics] subs]
            (alter consumers update consumer
                   rebalance (topic-partitions database topics) :remove client)
            (alter clients update client dissoc consumer)))))
    (ok)))

(defn- partition-offsets
  [offsets consumers groups client]
  (letfn [(partitions [g t c]
            (mapv (fn [p]
                    (let [k [g t p]]
                      (conj k (get offsets k))))
                  (get-in consumers [g t c])))]
    (reduce
      (fn [acc [g ts]]
        (reduce
          (fn [acc t]
            (apply conj acc (partitions g t client)))
          acc
          ts))
      []
      groups)))

(defn- load-messages
  [database [_ topic partition offset]]
  (d/chain
    (d/future (q.messages/load-messages database topic partition offset 100))
    (fn [messages]
      (mapv (fn [{:keys [id payload]}]
              {:topic     topic
               :partition partition
               :offset    id
               :payload   (String. payload "UTF-8")})
            messages))))

(defmethod handle :fetch
  [{:keys [database client-info] :as ctx}
   {:keys [consumers]}]
  (let [client     (:remote-addr client-info)
        groups     (cond-> (get @(:clients ctx) client)
                     (seq consumers) (select-keys consumers))
        partitions (log/spy :info (partition-offsets @(:offsets ctx)
                                                     @(:consumers ctx)
                                                     groups
                                                     client))
        load-fn    (partial load-messages database)
        messages   @(d/chain
                      (apply d/zip (map load-fn partitions))
                      (fn [result] (vec (flatten result))))]
    {:type     :fetch-response
     :messages messages}))

(defn- commit-offsets
  [store offsets]
  (log/info "Commit offsets:" offsets)
  (reduce
    (fn [acc {:keys [consumer topic partition offset]}]
      (assoc acc [consumer topic partition] offset))
    store
    offsets))

(defmethod handle :commit
  [{:keys [offsets]} packet]
  (swap! offsets commit-offsets (:offsets packet))
  (ok))

(defmethod handle :default
  [_ctx packet]
  (error 400 (str "Unknown packet: " (:type packet))))

(defn- save-offsets
  [database offsets snapshot]
  (let [raw-offsets  @offsets
        raw-snapshot @snapshot
        changed      (reduce
                       (fn [acc [k v]]
                         (if (not= v (get raw-snapshot k))
                           (conj acc (conj k v))
                           acc))
                       []
                       raw-offsets)]
    (when (seq changed)
      (log/debug "Saving offsets:" changed)
      (q.offsets/save-all database changed)
      (reset! snapshot raw-offsets))
    changed))

(defn- setup-offsets-saver
  [{:keys [database offsets snapshot]} interval]
  (let [stream (stream/periodically interval
                                    interval
                                    #(save-offsets database offsets snapshot))]
    (stream/consume (fn [changed]
                      (when (seq changed)
                        (log/info "Offsets saved:" changed)))
                    stream)
    stream))

(defrecord Broker [config database]
  component/Lifecycle
  (start [this]
    (log/info "Starting broker:" (:port config) "...")
    (let [clients   (ref {})
          consumers (ref {})
          offsets   (atom (q.offsets/load-all database))
          snapshot  (atom @offsets)
          ctx       {:database  database
                     :clients   clients
                     :consumers consumers
                     :offsets   offsets
                     :snapshot  snapshot}
          server    (start-server (connection ctx) (:port config))
          timer     (setup-offsets-saver ctx (:offsets-save-interval config))]
      (assoc this :server server
                  :clients clients
                  :consumers consumers
                  :offsets offsets
                  :snapshot snapshot
                  :timer timer)))
  (stop [{:keys [server offsets snapshot timer] :as this}]
    (log/info "Stopping broker ...")
    (when (some? server)
      (.close ^Closeable server))
    (when (some? timer)
      (stream/close! timer))
    (when (some? offsets)
      (save-offsets database offsets snapshot))
    (dissoc this :server :clients :consumers :offsets :snapshot :timer)))

(defn new-broker
  [config]
  (map->Broker {:config config}))


(comment

  (def broker (:broker (calyx.system/running-system)))

  @(:clients broker)
  @(:consumers broker)
  @(:offsets broker)
  @(:snapshot broker)

  (require '[pomanka.client.core :as client])

  (def c @(client/connect "localhost" 10000))
  (def c1 @(client/connect "localhost" 10000))
  (def c2 @(client/connect "localhost" 10000))

  (client/close c)

  @(client/subscribe c "hashtree" ["pg_change_records"])

  @(client/fetch c ["hashtree"])
  @(client/commit c [{:consumer  "hashtree"
                      :topic     "pg_change_records"
                      :partition 0
                      :offset    2}
                     {:consumer  "hashtree"
                      :topic     "pg_change_records"
                      :partition 1
                      :offset    5}])
  )

(comment

  ;; consumers
  {"group1" {"topic1" {:client1 '(0 1)}
             "topic2" {:client1 '(0 1)}}}

  ;; clients
  {:client1 {"group1" #{"topic1" "topic2"}}}

  ;; offsets
  {["group1" "topic1" 1] 100
   ["group1" "topic1" 2] 105
   ["group1" "topic2" 1] 200
   ["group1" "topic2" 2] 300}

  )
