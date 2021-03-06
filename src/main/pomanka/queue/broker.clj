(ns pomanka.queue.broker
  (:require
    [calyx.json :as json]
    [clojure.spec.alpha :as s]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [manifold.deferred :as d]
    [manifold.stream :as stream]
    [pomanka.database :as db]
    [pomanka.dumper.core :as dumper]
    [pomanka.queue.messages :as q.messages]
    [pomanka.queue.offsets :as q.offsets]
    [pomanka.queue.protocol :as protocol]
    [pomanka.queue.server :as server]
    [pomanka.queue.topics :as q.topics])
  (:import
    [java.io Closeable]
    [java.util List]
    [io.debezium.engine ChangeEvent DebeziumEngine$RecordCommitter]))


(s/def ::database ::db/config)
(s/def ::port pos-int?)
(s/def ::offsets-save-interval pos-int?)
(s/def ::config (s/keys :req-un [::database
                                 ::port
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
  (transduce
    (comp (map (fn [t] (q.topics/get-or-create-topic! database t)))
          (map (fn [{:topic/keys [name partitions]}] [name partitions])))
    (fn
      ([] [])
      ([result] result)
      ([result [x y]]
       (assoc result x y)))
    {}
    topics))

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

(defn- load-partition-messages
  [database highwater [_ topic partition offset]]
  (let [batch  100
        offset (or offset 0)
        high   (get-in @highwater [topic partition])]
    (if (or (nil? high)
            (< offset high))
      (d/chain
        (d/future (q.messages/load-messages database topic partition offset batch))
        (fn [messages]
          (let [messages (mapv (fn [{:keys [id payload]}]
                                 {:topic     topic
                                  :partition partition
                                  :offset    id
                                  :payload   (String. payload "UTF-8")})
                               messages)]
            (when (< (count messages) batch)
              (swap! highwater update topic assoc partition
                     (or (:offset (last messages)) offset)))
            messages)))
      [])))

(defn- load-messages
  [{:keys [database highwater waiting]} partitions timeout]
  (letfn [(load-partition [partition]
            (load-partition-messages database highwater partition))
          (do-load []
            @(d/chain
               (apply d/zip (map load-partition partitions))
               (fn [result] (vec (flatten result)))))]
    (let [msgs (do-load)]
      (if (or (seq msgs) (zero? timeout))
        msgs
        (let [condvar (Object.)
              topics  (->> partitions
                           (reduce (fn [acc [_ topic _ _]] (conj acc topic)) #{}))
              cj      #((fnil conj #{}) % condvar)]
          (swap! waiting #(reduce (fn [a t] (update a t cj))
                                  %
                                  topics))
          (locking condvar
            (.wait condvar timeout))
          (swap! waiting #(reduce (fn [a t] (update a t disj condvar))
                                  %
                                  topics))
          (do-load))))))

(defmethod handle :fetch
  [{:keys [client-info] :as ctx}
   {:keys [consumers timeout]}]
  (let [client     (:remote-addr client-info)
        groups     (cond-> (get @(:clients ctx) client)
                     (seq consumers) (select-keys consumers))
        partitions (partition-offsets @(:offsets ctx)
                                      @(:consumers ctx)
                                      groups
                                      client)
        messages   (load-messages ctx partitions timeout)]
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

(def table->topic-partition
  (memoize
    (fn [table]
      (let [parts (reverse (str/split table #"_"))]
        (when (> (count parts) 1)
          [(str/join "_" (reverse (rest parts)))
           (Integer/parseInt (first parts))])))))

(defn- handle-record
  [{:keys [highwater waiting]} ^ChangeEvent record]
  (when-let [{:keys [op source after]} (some-> (.value record)
                                               json/decode
                                               :payload)]
    (when (= op "c")
      (when-let [[topic partition] (table->topic-partition (:table source))]
        (swap! highwater update topic assoc partition (:id after))
        (when-let [condvars (get @waiting topic)]
          (doseq [v condvars]
            (locking v
              (.notifyAll v))))))))

(defn- dumper-handler
  [ctx]
  (fn [^List records
       ^DebeziumEngine$RecordCommitter committer]
    (doseq [record records]
      (handle-record ctx record)
      (.markProcessed committer record))))

(defn- create-dumper
  [ctx config]
  (let [dumper (dumper/create-dumper
                 {:name        "topic_highwater"
                  :source      (:database config)
                  :offset      {:storage "org.apache.kafka.connect.storage.MemoryOffsetBackingStore"}
                  :server-name "pomanka_topics"
                  :slot-name   "topic_highwater"
                  :publication "topic_highwater"
                  :extra       {"schema.include.list" "topics"
                                "slot.drop.on.stop"   "true"
                                "snapshot.mode"       "never"}}
                 (dumper-handler ctx))]
    (dumper/start dumper)
    dumper))

(defrecord Broker [config]
  component/Lifecycle
  (start [this]
    (log/info "Starting broker:" (:port config) "...")
    (let [database  (db/create-datasource (:database config))
          clients   (ref {})
          consumers (ref {})
          offsets   (atom (q.offsets/load-all database))
          snapshot  (atom @offsets)
          highwater (atom {})
          waiting   (atom {})
          ctx       {:database  database
                     :clients   clients
                     :consumers consumers
                     :offsets   offsets
                     :snapshot  snapshot
                     :highwater highwater
                     :waiting   waiting}
          server    (start-server (connection ctx) (:port config))
          timer     (setup-offsets-saver ctx (:offsets-save-interval config))
          dumper    (create-dumper ctx config)]
      (assoc this :database database
                  :server server
                  :clients clients
                  :consumers consumers
                  :offsets offsets
                  :snapshot snapshot
                  :timer timer
                  :highwater highwater
                  :waiting waiting
                  :dumper dumper)))
  (stop [{:keys [database server dumper offsets snapshot timer] :as this}]
    (log/info "Stopping broker ...")
    (when (some? dumper)
      (dumper/stop dumper))
    (when (some? server)
      (.close ^Closeable server))
    (when (some? timer)
      (stream/close! timer))
    (when (some? offsets)
      (save-offsets database offsets snapshot))
    (when (some? database)
      (db/close-datasource database))
    (dissoc this
            :database
            :server
            :clients
            :consumers
            :offsets
            :snapshot
            :timer
            :dumper
            :highwater
            :waiting)))

(defn new-broker
  [config]
  (map->Broker {:config config}))

(s/def ::broker (partial instance? Broker))

(comment

  (def broker (:broker (calyx.system/running-system)))

  @(:clients broker)
  @(:consumers broker)
  @(:offsets broker)
  @(:snapshot broker)
  @(:highwater broker)
  @(:waiting broker)

  (require '[pomanka.queue.client :as client])

  (def c @(client/connect "localhost" 10000))
  (def c1 @(client/connect "localhost" 10000))
  (def c2 @(client/connect "localhost" 10000))

  (client/close c)

  @(client/subscribe c "hashtree" ["pg_change_records"])

  (def msgs (future @(client/fetch c ["hashtree"] 500000)))

  @msgs

  @(client/commit c [{:consumer  "hashtree"
                      :topic     "pg_change_records"
                      :partition 0
                      :offset    3}
                     ])
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
