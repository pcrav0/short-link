(ns main
  (:gen-class)
  (:require
   ;; [clj-java-decompiler.core :refer [decompile]]
   [clojure.pprint :refer [pprint]]
   [ring.adapter.jetty :refer [run-jetty]]
   [ring.middleware.reload :refer [wrap-reload]]
   [hiccup.middleware :refer [wrap-base-url]]
   [hiccup.form :as form]
   [hiccup2.core :as hiccup2]
   [hiccup.core :as hiccup]
   [bidi.ring :refer [make-handler]]
   [ring.util.response :refer [redirect]]
   [ring.middleware.params :refer [wrap-params]]
   [clojure.core.async :refer [chan <!! >!! <! go-loop]]
   [clojure.java.jdbc :as jdbc]
   [clojure.string :refer [lower-case]]))

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
     {"" (fn [_]
           {:status 200
            :body (form/form-to [:post "/"]
                                (form/label "link" "link:")
                                (form/text-field "link"))})

      ["c/" :link] (fn [{{:keys [link]} :route-params}]
                     {:status 200
                      :body [:a {:href (str "/" link)} link]})

      true (fn [{:keys [uri] :as request}]
             (let [out (chan)]
               (>!! event-queue {:type :require
                                 :code (-> uri rest clojure.string/join)
                                 :out out})
               (redirect (<!! out) 303))
             #_(redirect
                (-> (jdbc/query db-spec
                                ["select link from links where code = ?"
                                 (-> uri rest clojure.string/join)])
                    first
                    :link
                    (or "/"))
                303))}

     :post
     {"" (fn [{{:strs [^String link]} :params}]
           (>!! event-queue {:type :insert :link link})
           (redirect (str "/c/" (encode62-str link)) 303))}}

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
               (update :body #(str (hiccup2/html %)))
               (update :headers (partial merge {"Content-Type" "text/html"}))))
         #_(fn [handler request]
             (let [respose (handler request)]
               (pprint request)
               (pprint respose)
               respose)))))

(defonce server
  (run-jetty (wrap-reload #'app) {:port 8080 :join? false}))

(def consumer
  (go-loop []
    (let [event (<! event-queue)]
      (condp = (:type event)
        :insert
        (let [{:keys [link]} event]
          (pprint event)
          (println "saving" link)
          (jdbc/insert! db-spec :links {:code (encode62-str link) :link link}))

        :require
        (let [{:keys [code out]} event]
          (println "requiring")
          (>!! out (-> (jdbc/query
                        db-spec
                        ["select link from links where code = ?" code])
                       first
                       :link
                       (or "/"))))))

    (recur)))

(defn server-restart! []
  (doto server
    (.stop)
    (.start)))

(defn -main []
  (println "oi"))
