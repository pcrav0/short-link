(ns main
  (:gen-class)
  (:require
   [clojure.pprint :refer [pprint pprint-newline]]
   [ring.adapter.jetty :refer [run-jetty]]
   [ring.middleware.reload :refer [wrap-reload]]
   [ring.middleware.params :refer [assoc-form-params]]
   [hiccup.middleware :refer [wrap-base-url]]
   [hiccup.form :as form]
   [hiccup2.core :as hiccup2]
   [hiccup.core :as hiccup]
   [bidi.ring :refer [make-handler]]
   ;; [clj-java-decompiler.core :refer [decompile]]
   ))

(defn pipe [& fs]
  (reduce (fn [acc f] (partial f acc)) fs))

(def routes
  ["/"
   {""
    {:get
     {"" (fn [_]
           {:status 200
            :body (form/form-to [:post "/"]
                                (form/label "link" "link:")
                                (form/text-field "link"))})}

     :post
     {"" (fn [{{:keys [link]} :params}]
           (println link)
           (ring.util.response/redirect "/" 303))}}

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

(defn server-restart! []
  (doto server
    (.stop)
    (.start)))

(defn -main []
  (println "oi"))
