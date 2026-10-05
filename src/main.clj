(ns main
  (:gen-class)
  (:require
   ;; [clj-java-decompiler.core :refer [decompile]]
   [clojure.pprint :refer [pprint]]
   [ring.adapter.jetty :refer [run-jetty]]
   [ring.middleware.reload :refer [wrap-reload]]
   [hiccup.middleware :refer [wrap-base-url]]
   [hiccup.page :as page]
   [hiccup.form :as form]
   [hiccup2.core :as hiccup2]
   [hiccup.core :as hiccup]
   [bidi.ring :refer [make-handler]]
   [ring.util.response :refer [redirect]]
   [ring.util.codec :refer [url-encode]]
   [ring.middleware.params :refer [wrap-params]]
   [clojure.core.async :refer [chan <!! >!! <! go-loop]]
   [clojure.java.jdbc :as jdbc]
   [clojure.string :refer [lower-case]]
   [clojure.java.io :as file]))

(defn pipe [init & fs]
  (reduce (fn [acc f] (partial f acc)) init fs))

(defn sqlize
  "Convert string/symbol/keyword to string with dashes replaced by underscores."
  [s]
  (when s
    (-> ^String (if (or (symbol? s) (keyword? s))
                  (name s)
                  (str s))
        (.replace "-" "_")
        (lower-case))))

(defn drop!
  "Drop a given table."
  [db-spec table-key]
  (->> (jdbc/drop-table-ddl (sqlize table-key))
       (vector)
       (jdbc/execute! db-spec))
  db-spec)

(defn exists?
  "Check whether a given table exists."
  [db-spec table-key]
  (try
    (do
      (->> (format "select 1 from %s" (sqlize table-key))
           (vector)
           (jdbc/query db-spec))
      true)
    (catch Throwable ex
      false)))

(defn drop-if-exists!
  "Drop table if it exists."
  [db-spec table-key]
  (when (exists? db-spec table-key)
    (drop! db-spec table-key))
  db-spec)

(def event-queue (chan 10))

(def db-spec {:subprotocol "sqlite"
              :classname "org.sqlite.JDBC"
              :subname "here.sql"})

(defn event-hook-out! [event]
  (let [out (chan)]
    (>!! event-queue (assoc event :out out))
    (<!! out)))

(defn start-db! []
  (Class/forName "org.sqlite.JDBC")
  (drop-if-exists! db-spec :links)
  (jdbc/db-do-commands
   db-spec
   (jdbc/create-table-ddl :links [[:code :text "not null"]
                                  [:link :text]])))

(defn encode62 [n]
  (if (zero? n)
    "0"
    (let [base62 (->> [[\0 \9] [\a \z] [\A \Z]]
                      (map (fn [[start end]] [(int start) (-> end int inc)]))
                      (map (partial apply range))
                      flatten
                      (map char))
          l (count base62)]
      (loop [s "" i n]
        (if (zero? i)
          s
          (recur (str (nth base62 (mod i l)) s)
                 (quot i l)))))))

(defn encode62-str [^String s]
  (-> s hash encode62))

(def routes
  ["/"
   {""
    {:get
     {#{""} (fn [_]
              {:status 200
               :body (form/form-to [:post "/"]
                                   (form/label "link" "link:")
                                   (form/text-field "link"))})

      ["c/" :link] (fn [{{:keys [link]} :route-params}]
                     (event-hook-out! {:type :page :code link}))

      true (fn [{:keys [uri] :as request}]
             (event-hook-out! {:type :require :code (-> uri rest clojure.string/join)}))}

     :post
     {"" (fn [{{:strs [^String link]} :params}]
           (event-hook-out! {:type :insert :link link}))}}

    true
    (fn [request]
      {:status 404 :body [:h1 "not found"]})}])

(def app
  (-> routes
      (make-handler wrap-params)
      wrap-base-url
      wrap-reload
      (pipe
       (fn [handler request]
         (-> (handler request)
             (update :body
                     #(str
                       (page/html5
                           (page/include-css "https://cdn.jsdelivr.net/npm/holiday.css@0.11.6")
                           %)))
             (update :headers (partial merge {"Content-Type" "text/html"})))))))

(defonce server
  (run-jetty (wrap-reload #'app) {:port 8080 :join? false}))

(def consumer
  (go-loop []
    (let [event (<! event-queue)]
      (condp = (:type event)
        :insert
        (let [{:keys [link out]} event
              encoded (encode62-str link)]
          (jdbc/insert! db-spec :links {:code encoded :link link})
          (>!! out (redirect (str "/c/" encoded) 303)))

        :page
        (let [{:keys [code out]} event]
          (>!! out
               {:status 200
                :body [:div {:style "display: flex; justify-content: center; align-items: center; text-align: center;"}
                       [:div
                        [:a {:href "/"} "index"]
                        [:hr]
                        (-> (jdbc/query
                             db-spec
                             ["select link from links where code = ?" code])
                            first
                            :link
                            (if [:div
                                 [:a {:id "link" :href (str "/" code)}
                                  (str "your link: " (url-encode code))]
                                 [:br]
                                 [:button {:onclick "navigator.clipboard.writeText(document.getElementById('link').href); alert(':)')"}
                                  "copy link"]]
                              [:p (str "cant find link: " (url-encode code))]))]]}))

        :require
        (let [{:keys [code out]} event]
          (println "requiring" event)
          (>!! out
               (if (= code "favicon.ico")
                 {:status 404}
                 (-> (jdbc/query
                      db-spec
                      ["select link from links where code = ?" code])
                     first
                     :link
                     (or "/")
                     (redirect 303)))))))

    (recur)))

(defn server-restart! []
  (doto server
    (.stop)
    (.start)))

(defn now []
  (.getEpochSecond (java.time.Instant/now)))

(defn -main []
  (def start-time (now))
  (def db-file (file/file "./here.sql"))

  (when-not (.exists db-file)
    (start-db!))

  (println "type 'help' to get a list of actions")

  (while true
    (let [cmd (read-line)]
      (condp = cmd
        "help"
        (do
          (println "restart: restart the server")
          (println "delete: delete the database")
          (println "quit: exit from server")
          (println "status: current status of the server"))

        "status"
        (do
          (println "---")
          (println "started at:" (-> start-time (* 1000) java.util.Date. str))
          (println "time alive:" (-> java.time.LocalTime/MIN
                                     (.plusSeconds (- (now) start-time))
                                     str)))

        "restart" (server-restart!)
        "delete" (.delete db-file)
        "quit" (System/exit 0)))))
