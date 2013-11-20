(ns mhub.core
  (:require irclj.core)
  (:require [clj-kafka.consumer.zk :as kafka.c])
  (:require [clj-kafka.producer :as kafka.p])
  (:require [clojure.data.json :as json])
  (:use [metrics.gauges :only [gauge]])
  (:use [metrics.core :only [report-to-console]])
  (:use [metrics.meters :only [meter mark!]])
  (:use [metrics.histograms :only [histogram update!]])
  (:use [metrics.timers :only [timer time!]])
  (:require [com.stuartsierra.component :as c]))

(def consumer-config {"zookeeper.connect" "localhost:2181"
                      "group.id" "mhub"
                      "auto.offset.reset" "smallest"
                      "auto.commit.enable" "true"})

(def producer-config {"metadata.broker.list" "localhost:9092"})

(defn dev-system [] {:host "irc.freenode.net"
                     :port 6667
                     :channel "##pace"
                     :name "mhub-test"
                     :pconfig producer-config
                     :cconfig consumer-config})

(defn log-message [reader msg]
  (try
    (time! (:message-received-timer reader)
           (kafka.p/send-message (:producer (:bus reader)) (kafka.p/message "received" (.getBytes (json/json-str {:message msg}))))
           (gauge "messages-received" (swap! (:messages-received reader) inc))
           (mark! (:message-meter-received reader))
           (metrics.histograms/update! (:message-size-histo-received reader) (count (:text msg))))
    (catch Exception e (println (.getMessage e))))
  "")

(defn send-irc-message [message-writer message]
  (time! (:message-sent-timer message-writer)
         (irclj.core/message (:irc (:irc message-writer)) (:channel (:irc message-writer)) message)
         (gauge "messages-sent" (swap! (:messages-sent message-writer) inc))
         (mark! (:message-meter-sent message-writer))
         (update! (:message-size-histo-sent message-writer) (count message))))

(defrecord IRC [host port name channel bus irc]
  c/Lifecycle
  (start [c]
    (if irc
      c
      (let [irc (irclj.core/connect
                 host
                 port
                 name
                 :callbacks {:raw-log irclj.events/stdout-callback
                             :privmsg (fn [irc msg] (log-message c msg))})]
        (when @(:ready? @irc)
          (irclj.core/join irc channel))
        (assoc c :irc irc))))
  (stop [c]
    (if (not irc)
      c
      (do (irclj.core/quit (:irc c))
          (irclj.core/kill (:irc c))
          (assoc c :irc nil)))))

(defrecord MessageWriter [irc bus writer]
  c/Lifecycle
  (start [c]
    (if writer
      c
      (assoc c
        :writer
        (future
          (doall
           (for [msg (kafka.c/messages (:consumer bus) ["send"])]
             (send-irc-message c (String. (:value msg) "UTF-8"))))))))
  (stop [c]
    (if (not writer)
      c
      (do (future-cancel writer)
          (assoc c :writer nil)))))

(defrecord MessageBus [pconfig cconfig producer consumer]
  c/Lifecycle
  (start [c]
    (let [c (if producer
              c
              (assoc c :producer (kafka.p/producer pconfig)))]
      (if consumer
        c
        (assoc c
          :consumer
          (kafka.c/consumer cconfig)))))
  (stop [c]
    (let [c (if (not producer)
              c
              (do (.close producer)
                  (assoc c :producer nil)))]
      (if (not consumer)
        c
        (do (kafka.c/shutdown consumer)
            (assoc c :consumer nil))))))

(def mhub-system-components [:bus :irc :writer])

(defrecord Mhub [config bus irc writer]
  c/Lifecycle
  (start [s]
    (c/start-system s mhub-system-components))
  (stop [s]
    (c/stop-system s mhub-system-components)))

(defn irc [options]
  (map->IRC (into {:messages-received (atom 0)
                   :message-meter-received (meter "message-received" "messages")
                   :message-size-histo-received (histogram "message-length-received")
                   :message-received-timer (timer "message-received-time")}
                  options)))

(defn bus [options]
  (map->MessageBus options))

(defn message-writer []
  (map->MessageWriter {:messages-sent (atom 0)
                       :message-meter-sent (meter "message-sent" "messages")
                       :message-size-histo-sent (histogram "message-length-sent")
                       :message-sent-timer (timer "message-sent-time")}))

(defn mhub [config]
  (map->Mhub {:config config
              :irc (c/using (irc (select-keys config [:host :port :name :channel]))
                            [:bus])
              :bus (bus (select-keys config [:pconfig :cconfig]))
              :writer (c/using (message-writer)
                               [:bus :irc])}))

