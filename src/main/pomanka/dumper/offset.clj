(ns pomanka.dumper.offset
  (:require
    [next.jdbc :as jdbc]
    [next.jdbc.prepare :as prep]
    [pomanka.database :as db])
  (:import
    [java.nio ByteBuffer]
    [java.util Collection Map HashMap]
    [java.util.concurrent Future Executors ExecutorService TimeUnit]
    [org.apache.kafka.connect.errors ConnectException]
    [org.apache.kafka.connect.runtime WorkerConfig]
    [org.apache.kafka.connect.util Callback]))


(gen-class
  :name pomanka.bottlewater.offset.PostgresOffsetBackingStore
  :implements [org.apache.kafka.connect.storage.OffsetBackingStore]
  :state state
  :init init)

(defn -init
  []
  [[] (atom {:config   nil
             :database nil
             :executor nil
             :cache    nil})])

(defn- load-all
  [database]
  (let [data (db/execute! database {:select [:*]
                                    :from   [:bw_offsets]})]
    (->> data
         (map (fn [{:bw_offset/keys [key value]}]
                [(ByteBuffer/wrap key) (ByteBuffer/wrap value)]))
         (into {}))))

(defn- save-all
  [database values]
  (jdbc/with-transaction [tx database]
    (with-open [ps (jdbc/prepare
                     tx
                     ["INSERT INTO bw_offsets (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = ?"])]
      (prep/execute-batch! ps
                           (->> values
                                (map (fn [[k v]] [k v v]))
                                (into []))))))

(defn -start
  [this]
  (let [state    (.state this)
        {:keys [config]} @state
        database (db/create-datasource (assoc config :pool-size 1))
        cache    (load-all database)]
    (swap! state assoc
           :database database
           :executor (Executors/newSingleThreadExecutor)
           :cache cache)))

(defn -stop
  [this]
  (let [state (.state this)
        {:keys [database ^ExecutorService executor]} @state]
    (when (some? database)
      (db/close-datasource database))
    (when (some? executor)
      (.shutdown executor)
      (try
        (.awaitTermination executor 30 TimeUnit/SECONDS)
        (catch InterruptedException _ex
          (.interrupt (Thread/currentThread))))
      (when-not (.isEmpty (.shutdownNow executor))
        (throw (ConnectException.
                 (str "Failed to stop PostgresOffsetBackingStore. Exiting without "
                      "cleanly shutting down pending tasks and/or callbacks.")))))
    (swap! state assoc :database nil :executor nil)))

(defn ^Future -get
  [this ^Collection keys]
  (let [state                     (.state this)
        ^ExecutorService executor (get @state :executor)]
    (.submit executor
             ^Callable
             (reify Callable
               (call [_this]
                 (let [cache  (get @state :cache)
                       result (HashMap.)]
                   (doseq [k keys]
                     (.put result k (get cache k)))
                   result))))))

(defn ^Future -set
  [this ^Map values ^Callback callback]
  (let [state                     (.state this)
        ^ExecutorService executor (get @state :executor)]
    (.submit executor
             ^Callable
             (reify Callable
               (call [_this]
                 (swap! state update :cache
                        (fn [cache]
                          (reduce
                            (fn [acc [k v]] (assoc acc k v))
                            cache
                            values)))
                 (save-all (get @state :database) values)
                 (when (some? callback)
                   (.onCompletion callback nil nil))
                 nil)))))

(defn- config-key [k] (str "offset.storage.postgres." k))

(defn -configure
  [this ^WorkerConfig config]
  (let [state  (.state this)
        ^Map m (.originals config)]
    (swap! state assoc :config
           {:hostname (.get m (config-key "hostname"))
            :port     (Integer/parseInt (.get m (config-key "port")))
            :username (.get m (config-key "username"))
            :password (.get m (config-key "password"))
            :dbname   (.get m (config-key "dbname"))
            :table    (.get m (config-key "table"))})))
