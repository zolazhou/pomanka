(ns pomanka.queue.server
  (:require
    [potemkin :as p]
    [manifold.stream :as s]
    [aleph.netty :as netty]
    [clojure.tools.logging :as log])
  (:import
    [java.io IOException]
    [java.net InetSocketAddress]
    [io.netty.channel Channel
                      ChannelHandler
                      ChannelPipeline]
    [io.netty.handler.ssl SslHandler]))


(defn channel-remote-address [^Channel ch]
  (let [^InetSocketAddress addr (.remoteAddress ch)]
    {:ip   (some-> addr .getAddress .getHostAddress)
     :port (some-> addr .getPort)}))

(p/def-derived-map TcpConnection [^Channel ch]
  :server-name (netty/channel-server-name ch)
  :server-port (netty/channel-server-port ch)
  :remote-addr (channel-remote-address ch)
  :ssl-session (some-> ch ^ChannelPipeline (.pipeline) ^SslHandler (.get "ssl-handler") .engine .getSession))

(alter-meta! #'->TcpConnection assoc :private true)

(defn- ^ChannelHandler server-channel-handler
  [handler {:keys [raw-stream?]}]
  (let [in (atom nil)]
    (netty/channel-inbound-handler

      :exception-caught
      ([_ ctx ex]
       (when-not (instance? IOException ex)
         (log/warn ex "error in TCP server")))

      :channel-inactive
      ([_ ctx]
       (s/close! @in)
       (.fireChannelInactive ctx))

      :channel-active
      ([_ ctx]
       (let [ch (.channel ctx)]
         (handler
           (doto
             (s/splice
               (netty/sink ch true netty/to-byte-buf)
               (reset! in (netty/source ch)))
             (reset-meta! {:aleph/channel ch}))
           (->TcpConnection ch)))
       (.fireChannelActive ctx))

      :channel-read
      ([_ ctx msg]
       (netty/put! (.channel ctx) @in
                   (if raw-stream?
                     msg
                     (netty/release-buf->array msg)))))))

(defn start-server
  "Takes a two-arg handler function which for each connection will be called with a duplex
   stream and a map containing information about the client.  Returns a server object that can
   be shutdown via `java.io.Closeable.close()`, and whose port can be discovered via `aleph.netty.port`.

   |:---|:-----
   | `port` | the port the server will bind to.  If `0`, the server will bind to a random port.
   | `socket-address` | a `java.net.SocketAddress` specifying both the port and interface to bind to.
   | `ssl-context` | an `io.netty.handler.ssl.SslContext` object. If a self-signed certificate is all that's required, `(aleph.netty/self-signed-ssl-context)` will suffice.
   | `bootstrap-transform` | a function that takes an `io.netty.bootstrap.ServerBootstrap` object, which represents the server, and modifies it.
   | `pipeline-transform` | a function that takes an `io.netty.channel.ChannelPipeline` object, which represents a connection, and modifies it.
   | `raw-stream?` | if true, messages from the stream will be `io.netty.buffer.ByteBuf` objects rather than byte-arrays.  This will minimize copying, but means that care must be taken with Netty's buffer reference counting.  Only recommended for advanced users."
  [handler
   {:keys [port socket-address ssl-context bootstrap-transform pipeline-transform epoll?]
    :or   {bootstrap-transform identity
           pipeline-transform  identity
           epoll?              false}
    :as   options}]
  (netty/start-server
    (fn [^ChannelPipeline pipeline]
      (.addLast pipeline "handler" (server-channel-handler handler options))
      (pipeline-transform pipeline))
    ssl-context
    bootstrap-transform
    nil
    (if socket-address
      socket-address
      (InetSocketAddress. port))
    epoll?))
