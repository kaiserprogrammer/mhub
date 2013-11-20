(ns mhub.core
  (:require irclj.core)
  (:require [clj-kafka.consumer.zk :as kafka.c])
  (:require [clj-kafka.producer :as kafka.p])
  (:require [clojure.data.json :as json])
  (:use [metrics.gauges :only [gauge]])
  (:use [metrics.core :only [report-to-console]])
  (:use [metrics.meters :only [meter mark!]])
  (:use [metrics.histograms :only [histogram update!]])
  (:use [metrics.timers :only [timer time!]]))

(def config {"zookeeper.connect" "localhost:2181"
             "group.id" "mhub"
             "auto.offset.reset" "smallest"
             "auto.commit.enable" "true"})

(defn dev-system [] (atom
                     {:host "irc.freenode.net"
                      :port 6667
                      :channel "##pace"
                      :name "mhub-test"
                      :irc nil
                      :bus config
                      :consumer nil
                      :producer nil
                      :message-writer nil
                      :messages-sent (atom 0)
                      :messages-received (atom 0)
                      :message-meter-received (meter "message-received" "messages")
                      :message-meter-sent (meter "message-sent" "messages")
                      :message-size-histo-received (histogram "message-length-received")
                      :message-size-histo-sent (histogram "message-length-sent")
                      :message-received-timer (timer "message-received-time")
                      :message-sent-timer (timer "message-sent-time")}))

(defn start-message-bus [sys]
  (swap! sys assoc :producer
         (kafka.p/producer {"metadata.broker.list" "localhost:9092"}))
  (swap! sys assoc :consumer
         (kafka.c/consumer (:bus @sys))))

(defn stop-message-bus [sys]
  (let [p (:producer @sys)]
    (when p (.close p)))
  (let [c (:consumer @sys)]
    (when c (kafka.c/shutdown c))))

(defn log-message [system irc msg]
  (try
    (time! (:message-received-timer system)
           (kafka.p/send-message (:producer system) (kafka.p/message "received" (.getBytes (json/json-str {:message msg}))))
           (gauge "messages-received" (swap! (:messages-received system) inc))
           (mark! (:message-meter-received system))
           (metrics.histograms/update! (:message-size-histo-received system) (count (:text msg))))
    (catch Exception e (println (.getMessage e))))
  "")

(defn start-irc [sys]
  (let [system @sys]
    (let [irc (irclj.core/connect (:host system) (:port system) (:name system) :callbacks {:raw-log irclj.events/stdout-callback :privmsg (fn [irc msg] (log-message system irc msg))})]
      (swap! sys assoc :irc irc))
    (when @(:ready?
            @(:irc @sys))
      (irclj.core/join (:irc @sys) (:channel system)))))

(defn send-irc-message [system message]
  (time! (:message-sent-timer system)
         (irclj.core/message (:irc system) (:channel system) message)
         (gauge "messages-sent" (swap! (:messages-sent system) inc))
         (mark! (:message-meter-sent system))
         (update! (:message-size-histo-sent system) (count message))))

(defn stop-irc [sys]
  (when (:irc @sys)
    (irclj.core/quit (:irc @sys))
    (irclj.core/kill (:irc @sys))))

(defn start-message-writer [sys]
  (swap! sys assoc :message-writer
         (future
           (doall
            (for [msg (kafka.c/messages (:consumer @sys) ["send"])]
              (send-irc-message @sys (String. (:value msg) "UTF-8")))))))

(defn stop-message-writer [sys]
  (let [f (:message-writer @sys)]
    (when f
      (future-cancel f))))

(defn start [system]
  (start-message-bus system)
  (start-irc system)
  (start-message-writer system))

(defn stop [system]
  (stop-irc system)
  (stop-message-bus system)
  (stop-message-writer system))
