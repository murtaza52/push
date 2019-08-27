(ns hs.push
  (:require [ezzmq.core :as zmq]
            [ezzmq.util :as zmqu]
            [ezzmq.context :as zmqc]
            [clojure.core.async :as a :refer [go go-loop chan <! put! >!]]
            [hs.util :refer [while-let]]))


(defn- bind-socket
  [socket address]
  (.bind socket address))

;; backend
(defn start-publisher
  [{:keys [address in register-fn] :or {register-fn identity}}]
  (let [context (zmqc/context)]
    (zmqc/init-context! context)
    (let [socket (zmq/create-socket context (zmq/socket-types :pub))]
      (bind-socket socket address)
      (go
        (while-let [msg (<! in)]
          (zmq/send-msg socket msg))
        (zmqc/shut-down-context! context)))))

(def in (chan 10))

(defn clean-backend
  [ch]
  (a/close! ch))


(defn frames->str
  [frames]
  (mapv #(apply str (map char %))
        frames))


(defn- connect
  [socket address]
  (.connect socket address))

(defn- subscribe
  [socket topic]
  (.subscribe socket (zmqu/as-byte-array topic)))

(defn start-subscriber
  [pub]
  (let [context (zmqc/context)
        _ (zmqc/init-context! context)
        ;; create a subscription socket and subscribe to an invalid topic (or else by default the socket will be subscribed to all topics)
        socket (zmq/create-socket context (zmq/socket-types :sub))
        ;; channel on which incoming msgs will be put
        out (chan 10)
        ;; each msg has 2 frames, where the first frame corresponds to the topic
        publication (a/pub out first)
        ;; determines if the subscriber should shut down
        receive-msgs? (volatile! true)
        ;; will be called to un-subscribe from a topic
        make-unsubscribe-fn (fn [topic ch]
                              #(a/unsub publication topic ch)
                              (a/close! ch))
        subscribe-socket (partial subscribe socket)]

    ;; go process waiting for any new publishers
    (go
      (while-let [address (<! pub)]
        ;; connect the subscriber to the new publisher
        (connect socket address)))

    ;; a new thread is started for every new subscriber
    (future
      (while @receive-msgs?
        ;; this will block till a msg arrives
        (let [msg (frames->str (zmq/receive-msg socket))]
          (println (str "received msg - " msg))
          (a/>!! out msg))))

    ;; fn that will be called for a new subscription
    {:subscriber (fn [topic]
                   ;; subscribe the socket to the topic
                   (subscribe-socket topic)
                   (let [ch (chan 50 (map rest))]
                     ;; subscribe the client channel to the topic
                     (a/sub publication topic ch)
                     {:out ch
                      :unsubscribe-fn (make-unsubscribe-fn topic ch)}))
     :destroy-socket (fn []
                       (vreset! receive-msgs? false)
                       (zmqc/shut-down-context! context)
                       (a/unsub-all publication)
                       (a/close! out))}))


;; check what happens when destroy-socket is called while it is blocked on receive-msg


(comment

  ;; publisher
  (def address "tcp://*:12345")

  (def in (chan 10))

  (start-publisher {:in in :address address})

  ;; subscriber

  ;; value from config
  (def publisher-addresses ["tcp://*:12345"])

  (def add-publisher (chan 50))

  ;; start subscriber
  (def subscriber (start-subscriber add-publisher))

  ;; add publishers
  (go
    (doseq [v publisher-addresses]
      (>! add-publisher v)))

  ;; a client process
  (go
    (let [{:keys [out unsubscribe-fn]} ((subscriber :subscriber) "user-1")]
      (dotimes [_ 10]
        (println "reading msg from out")
        (println (<! out)))))

  (let [{:keys [out unsubscribe-fn]} ((subscriber :subscriber) "user-52")]
    (go
      (while-let [msg (<! out)]
        (println msg))
      (unsubscribe-fn)))

  ;; publish stuff
  (put! in ["user-53" "hi"])



  )
