(ns pomanka.core
  (:gen-class)
  (:require
    [clojure.tools.cli :as cli]
    [reloaded.repl :refer [set-init! init start stop]]
    [pomanka.system :refer [prod-system]]))


(def cli-options
  [["-c" "--config CONFIG" "Configuration file"]
   ["-h" "--help"]])

(defn stop-app []
  (stop)
  (shutdown-agents))

(defn start-app [{options :options}]
  (.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable stop-app))
  (set-init! #(prod-system (:config options)))
  (init)
  (start))

(defn -main [& args]
  (let [args (cli/parse-opts args cli-options)]
    (if-some [errors (:errors args)]
      (doseq [e errors] (println e))
      (start-app args))))
