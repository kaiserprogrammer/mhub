(ns mhub.core
  (:require irclj.core)
  (:require [clj-kafka.consumer.zk :as kafka.c])
  (:require [clj-kafka.producer :as kafka.p])
  (:require [clojure.data.json :as json]))

(def config {"zookeeper.connect" "localhost:2181"
             "group.id" "mhub"
             "auto.offset.reset" "smallest"
             "auto.commit.enable" "false"})

(defn dev-system [] {:host "irc.freenode.net"
                     :port 6667
                     :channel "##pace"
                     :name "mhub-test"
                     :irc (ref nil)
                     :bus config
                     :consumer (atom nil)
                     :producer (atom nil)
                     :message-writer (atom nil)})

(defn start-message-bus [system]
  (swap! (:producer system)
         (fn [_] (kafka.p/producer {"metadata.broker.list" "localhost:9092"})))
  (swap! (:consumer system)
         (fn [_] (kafka.c/consumer (:bus system)))))

(defn stop-message-bus [system]
  (swap! (:producer system)
         (fn [p] (when p (.close p)) nil))
  (swap! (:consumer system)
         (fn [c] (when c (kafka.c/shutdown c)) nil)))

(defn log-message [system irc msg]
  (try
    (kafka.p/send-message @(:producer system) (kafka.p/message "received" (.getBytes (json/json-str {:message msg}))))
    (catch Exception e (println (.getMessage e))))
  "")

(defn start-irc [system]
  (let [irc (irclj.core/connect (:host system) (:port system) (:name system) :callbacks {:raw-log irclj.events/stdout-callback :privmsg (partial log-message system)})]
    (dosync
     (alter (:irc system) #(identity %2) irc)))
  (when @(:ready? @@(:irc system))
    (irclj.core/join (deref (:irc system)) (:channel system))))

(defn send-irc-message [system message]
  (irclj.core/message (deref (:irc system)) (:channel system) message))

(defn stop-irc [system]
  (irclj.core/quit (deref (:irc system)))
  (irclj.core/kill (deref (:irc system))))

(defn start-message-writer [system]
  (swap! (:message-writer system)
         (fn [_]
           (future
             (doall
              (for [msg (kafka.c/messages @(:consumer system) ["send"])]
                (send-irc-message system (String. (:value msg) "UTF-8"))))))))

(defn stop-message-writer [system]
  (swap! (:message-writer system)
         (fn [f]
           (when f
             (future-cancel f))
           nil)))

(defn start [system]
  (start-message-bus system)
  (start-irc system)
  (start-message-writer system))

(defn stop [system]
  (stop-irc system)
  (stop-message-bus system)
  (stop-message-writer system))
