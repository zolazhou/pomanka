(ns pomanka.specs
  (:require
    [clojure.spec.alpha :as s]
    [clojure.spec.gen.alpha :as g]
    [clojure.string :as string]
    [com.fulcrologic.guardrails.core :refer [?]]))


(s/def ::non-empty-string
  (s/with-gen
    (s/and string? (complement string/blank?))
    #(g/not-empty (g/string-alphanumeric))))

(s/def ::string-vec
  (s/coll-of string?
             :kind vector?
             :into []
             :gen (fn [] (g/vector (s/gen ::non-empty-string) 1 10))))

(s/def ::date inst?)

;; All dates in ISO 8601 "combined date and time representation" format:
;; https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations
(def iso8601-re #"^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$")

(s/def ::iso8601
  (s/with-gen
    (s/and string? #(re-matches iso8601-re %))
    #(g/fmap (fn [[y M d H m s]]
               (str y "-"
                    (format "%02d" M) "-"
                    (format "%02d" d) "T"
                    (format "%02d" H) ":"
                    (format "%02d" m) ":"
                    (format "%02d" s) "Z"))
             (s/gen (s/tuple (s/int-in 1997 2046)
                             (s/int-in 1 12)
                             (s/int-in 1 29)
                             (s/int-in 0 23)
                             (s/int-in 0 59)
                             (s/int-in 0 59))))))

(s/def :iso8601/created-at ::iso8601)
(s/def :iso8601/updated-at ::iso8601)
(s/def :iso8601/ts ::iso8601)

;; Limit directory or file name to 260 character length
;; https://stackoverflow.com/questions/1880321/why-does-the-260-character-path-length-limit-exist-in-windows
(s/def ::fname (s/and string? #(< 0 (count %) 260)))

(s/def ::sha256
  (s/with-gen
    (s/and string? #(= 64 (count %)))
    #(g/fmap string/join (g/vector (g/char-alphanumeric) 64))))

(s/def ::salt
  (s/with-gen
    (s/and string? #(= 8 (count %)))
    #(g/fmap string/join (g/vector (g/char-alphanumeric) 8))))

(s/def ::secret
  (s/with-gen
    (s/and string? #(= 32 (count %)))
    #(g/fmap string/join (g/vector (g/char-alphanumeric) 32))))

(s/def ::port (s/and nat-int? #(< 0 % 0x10000)))

(s/def ::id pos-int?)

;; Cursor is prefixed with `d-` or `f-`, for directory and file respectively
(s/def ::cursor (s/and string? #(re-matches #"^[df]\-[0-9]+$" %)))

(s/def ::revision (s/int-in 0 2147483648))

(s/def :find/limit pos-int?)
(s/def :find/ts ::date)
(s/def :find/offset ::id)
(s/def :find/cursor ::cursor)
(s/def :find/reverse? boolean?)
(s/def :find/options (s/keys :opt-un [:find/limit
                                      :find/reverse?
                                      :find/offset]))


;; event


