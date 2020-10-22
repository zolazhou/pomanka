(ns user
  (:require [clojure.tools.namespace.repl :refer [set-refresh-dirs]]
            [reloaded.repl]
            [pomanka.config :as config]
            [pomanka.system :refer [app-system]]))


(set-refresh-dirs "src/main" "src/dev")

(defn- init []
  (app-system (config/load-config)))

(reloaded.repl/set-init! init)

;; Set up aliases so they don't accidentally
;; get scrubbed from the namespace declaration
(def start reloaded.repl/start)
(def stop reloaded.repl/stop)
(def go reloaded.repl/go)
(def reset reloaded.repl/reset)
(def reset-all reloaded.repl/reset-all)
