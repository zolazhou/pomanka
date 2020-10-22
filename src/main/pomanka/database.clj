(ns pomanka.database
  (:require
    [calyx.json :as json]
    [calyx.util :as util]
    [clojure.spec.alpha :as s]
    [clojure.spec.gen.alpha :as g]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [com.fulcrologic.guardrails.core :refer [>defn >defn- ? =>]]
    [com.stuartsierra.component :as component]
    [hikari-cp.core :as hikari]
    [honeysql.core :as sql]
    [inflections.core :as inflections]
    [next.jdbc :as jdbc]
    ;; https://github.com/seancorfield/next-jdbc/blob/develop/doc/tips-and-tricks.md#working-with-date-and-time
    [next.jdbc.date-time]
    [next.jdbc.prepare :as p]
    [next.jdbc.result-set :as rs]
    [pomanka.config :as config]
    [taoensso.encore :as encore])
  (:import
    [clojure.lang IPersistentVector IPersistentMap]
    [com.zaxxer.hikari HikariDataSource]
    [java.nio ByteBuffer]
    [java.sql PreparedStatement Date Timestamp Connection]
    [java.time LocalDate Instant OffsetDateTime]
    [org.postgresql.jdbc PgArray]
    [org.postgresql.util PGobject]))


(set! *warn-on-reflection* true)

;; spec ====================================================

(s/def ::datasource (partial instance? HikariDataSource))
(s/def ::connection (partial instance? Connection))
(s/def ::config ::config/database)

(s/def ::statement (s/or :sql-query vector? :sql-map map?))

(>defn create-datasource
  [{:keys [hostname port dbname driver username password pool-size]}]
  [::config => ::datasource]
  (let [jdbc-url (str "jdbc:postgresql://" hostname ":" port "/" dbname)]
    (hikari/make-datasource
      (cond-> {:jdbc-url          jdbc-url
               :username          username
               :password          password
               :maximum-pool-size pool-size}
        (some? driver) (assoc :driver-class-name driver)))))

(>defn close-datasource
  [ds]
  [::datasource => any?]
  (hikari/close-datasource ds))

;; component =============================================

(defrecord Database [config]
  component/Lifecycle
  (start [this]
    (log/info "Starting Database ...")
    (let [ds (create-datasource config)]
      (assoc this :ds ds)))
  (stop [{:keys [ds] :as this}]
    (log/info "Stopping Database ...")
    (when ds (hikari/close-datasource ds))
    (dissoc this :ds)))

(declare new-database)

(def database? (partial instance? Database))

(s/def ::database (s/with-gen
                    database?
                    #(g/fmap new-database (s/gen ::config))))

(>defn new-database [config]
  [::config => ::database]
  (map->Database {:config config}))

(s/def ::executable (s/or :datasource ::datasource
                          :database ::database
                          :connection ::connection))

;; api ==================================================

(def singular (encore/memoize
                #(inflections/singular
                   (str/lower-case %))))

(defn as-kebab-maps [rs opts]
  (rs/as-modified-maps
    rs
    (assoc opts :qualifier-fn singular
                :label-fn util/->kebab-case)))

(defn ->conn
  [executable]
  (if (database? executable)
    (:ds executable)
    executable))

(>defn execute!
  ([db stmt]
   [::executable ::statement => any?]
   (execute! db stmt nil))
  ([db stmt opts]
   [::executable ::statement (? map?) => any?]
   (let [stmt (if (vector? stmt) stmt (sql/format stmt))]
     (log/debug "Database execute SQL:" stmt)
     (jdbc/execute! (->conn db)
                    stmt
                    (merge {:builder-fn as-kebab-maps} opts)))))

(>defn execute-one!
  ([db stmt]
   [::executable ::statement => any?]
   (execute-one! db stmt nil))
  ([db stmt opts]
   [::executable ::statement (? map?) => any?]
   (let [stmt (if (vector? stmt) stmt (sql/format stmt))]
     (log/debug "Database execute-one SQL:" stmt)
     (jdbc/execute-one! (->conn db)
                        stmt
                        (merge {:builder-fn as-kebab-maps} opts)))))

(defmacro with-transaction
  [[sym db opts] & body]
  `(jdbc/with-transaction [~sym (->conn ~db) ~opts]
     ~@body))

;; =====================================================

(defn- to-pg-json [data json-type]
  (doto (PGobject.)
    (.setType json-type)
    (.setValue (json/encode data))))

(extend-protocol p/SettableParameter
  Instant
  (set-parameter [v ^PreparedStatement ps ^long i]
    (.setObject ps i (Timestamp/from v)))
  IPersistentVector
  (set-parameter [v ^PreparedStatement ps ^long i]
    (let [conn      (.getConnection ps)
          meta      (.getParameterMetaData ps)
          type-name (.getParameterTypeName meta i)]
      (if (#{"json" "jsonb"} type-name)
        (.setObject ps i (to-pg-json v type-name))
        (if-let [elem-type (when (= (first type-name) \_) (apply str (rest type-name)))]
          (.setObject ps i (.createArrayOf conn elem-type (to-array v)))
          (.setObject ps i v)))))
  IPersistentMap
  (set-parameter [v ^PreparedStatement ps ^long i]
    (let [meta (.getParameterMetaData ps)]
      (if-let [type-name (.getParameterTypeName meta i)]
        (.setObject ps i (to-pg-json v type-name))
        (.setObject ps i v))))
  ByteBuffer
  (set-parameter [v ^PreparedStatement ps ^long i]
    (.setBytes ps i (.array ^ByteBuffer v))))

(defn read-pg-vector
  "oidvector, int2vector, etc. are space separated lists"
  [s]
  (when (seq s) (str/split s #"\s+")))

(defn read-pg-array
  "Arrays are of form {1,2,3}"
  [s]
  (when (seq s)
    (when-let [[_ content] (re-matches #"^\{(.+)\}$" s)]
      (if-not (empty? content)
        (str/split content #"\s*,\s*") []))))

(defmulti read-pgobject
          "Convert returned PGobject to Clojure value."
          #(keyword (when % (.getType ^PGobject %))))

(defmethod read-pgobject :oidvector
  [^PGobject x]
  (when-let [val (.getValue x)]
    (mapv read-string (read-pg-vector val))))

(defmethod read-pgobject :int2vector
  [^PGobject x]
  (when-let [val (.getValue x)]
    (mapv read-string (read-pg-vector val))))

(defmethod read-pgobject :anyarray
  [^PGobject x]
  (when-let [val (.getValue x)]
    (vec (read-pg-array val))))

(defmethod read-pgobject :json
  [^PGobject x]
  (when-let [val (.getValue x)]
    (json/decode val)))

(defmethod read-pgobject :jsonb
  [^PGobject x]
  (when-let [val (.getValue x)]
    (json/decode val)))

(defmethod read-pgobject :default
  [^PGobject x]
  (.getValue x))

(extend-protocol rs/ReadableColumn
  Date
  (read-column-by-label ^LocalDate [^Date v _]
    (.toLocalDate v))
  (read-column-by-index ^LocalDate [^Date v _2 _3]
    (.toLocalDate v))

  Timestamp
  (read-column-by-label ^Instant [^Timestamp v _]
    (.toInstant v))
  (read-column-by-index ^Instant [^Timestamp v _2 _3]
    (.toInstant v))

  PGobject
  (read-column-by-label [v _]
    (read-pgobject v))
  (read-column-by-index [v _2 _3]
    (read-pgobject v))

  PgArray
  (read-column-by-label [^PgArray v _]
    (vec (.getArray v)))
  (read-column-by-index [^PgArray v _2 _3]
    (vec (.getArray v))))

(extend-protocol clojure.core/Inst
  OffsetDateTime
  (inst-ms* [inst] (.toEpochMilli
                     ^Instant
                     (.toInstant ^OffsetDateTime inst))))

