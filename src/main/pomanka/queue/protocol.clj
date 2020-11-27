(ns pomanka.queue.protocol
  (:require
    [gloss.core :as g :refer [defcodec]]
    [gloss.core.codecs :refer [enum]]
    [gloss.io :as io]
    [manifold.stream :as stream]))


(defcodec packet-type
  (enum :byte
        :ok
        :error
        :subscribe
        :unsubscribe
        :fetch
        :fetch-response                                     ;; fetch response
        :commit))

(defcodec string (g/string :utf-8 :delimiters ["\\0"]))

(defcodec topics (g/repeated string :prefix :byte))
(defcodec consumers (g/repeated string :prefix :byte))

(defcodec offset {:consumer  string
                  :topic     string
                  :partition :int16
                  :offset    :int64})
(defcodec offsets (g/repeated offset :prefix :int16))


(defcodec ok {:type :ok})
(defcodec error {:type    :error
                 :code    :int32
                 :message string})

(defcodec subscribe {:type     :subscribe
                     :consumer string                       ;; consumer group
                     :topics   topics})

(defcodec unsubscribe {:type      :unsubscribe
                       :consumers consumers})

(defcodec commit {:type    :commit
                  :offsets offsets})

(defcodec fetch {:type      :fetch
                 :consumers consumers
                 :timeout   :int32})

(defcodec message {:topic     string
                   :partition :int16
                   :offset    :int64
                   :payload   string})

(defcodec fetch-response
  {:type     :fetch-response
   :messages (g/repeated message :prefix :int16)})

(defcodec frame
  (g/header
    packet-type
    {:ok             ok
     :error          error
     :subscribe      subscribe
     :unsubscribe    unsubscribe
     :commit         commit
     :fetch          fetch
     :fetch-response fetch-response}
    :type))

(defn wrap-duplex-stream
  [s]
  (let [out (stream/stream)]
    (stream/connect
      (stream/map #(io/encode frame %) out)
      s)
    (stream/splice
      out
      (io/decode-stream s frame))))
