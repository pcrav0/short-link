(ns main
  (:gen-class)
  (:require
   ;; [clj-java-decompiler.core :refer [decompile]]
   [clojure.pprint :refer [pprint pprint-newline]]
   [ring.adapter.jetty :refer [run-jetty]]
   [ring.middleware.reload :refer [wrap-reload]]
   [ring.middleware.params :refer [assoc-form-params]]
   [hiccup.middleware :refer [wrap-base-url]]
   [hiccup.form :as form]
   [hiccup2.core :as hiccup2]
   [hiccup.core :as hiccup]
   [bidi.ring :refer [make-handler]]
   [clojure.core.async :refer [chan <!! >!! <! go-loop]]))

(defn pipe [init & fs]
  (reduce (fn [acc f] (partial f acc)) init fs))

(def input (chan 10))

(def db (atom {}))

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
             (ring.util.response/redirect
              (get @db (-> uri rest clojure.string/join) "/")
              303))}

     :post
     {"" (fn [{{:strs [^String link]} :params}]
           (>!! input link)
           (ring.util.response/redirect (str "/c/" (encode62-str link)) 303))}}

    true
    (fn [request]
      {:status 404 :body [:h1 "not found"]})}])

(def app
  (-> routes
      (make-handler ring.middleware.params/wrap-params)
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
    (let [link (<! input)]
      (println "saving" link)
      (swap! db assoc (encode62-str link) link))
    (recur)))

(defn server-restart! []
  (doto server
    (.stop)
    (.start)))

(defn -main []
  (println "oi"))
