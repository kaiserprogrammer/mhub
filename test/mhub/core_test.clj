(ns mhub.core-test
  (:require [clojure.test :refer :all]
            [mhub.core :refer :all]))

(defn dev-system [] {:host "irc.freenode.net" :port 6667 :channel "##pace" :name "mhub-test" :irc (ref nil)})

(deftest write-to-message-bus-and-read
  (testing "Write and read to message bus"
    (let [system (dev-system)]
      (start system)
      (send-irc-message system "Working fine")
      (stop system))))
