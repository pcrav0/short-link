(ns main
  (:gen-class)
  (:require
   [clojure.pprint :refer [pprint pprint-newline]]
   [ring.adapter.jetty :refer [run-jetty]]
   [ring.middleware.reload :refer [wrap-reload]]
   [hiccup.middleware :refer [wrap-base-url]]
   [hiccup2.core :as hiccup2]
   ;; [clj-java-decompiler.core :refer [decompile]]
   ))

(defn pipe [& fs]
  (reduce (fn [acc f] (partial f acc)) fs))

(def app
  (-> (fn [request]
        {:status 200 :body [:p "oi"]})
      wrap-base-url
      wrap-reload
      (pipe
       (fn [handler request]
         (update (handler request) :body #(str (hiccup2/html %)))))))

(defonce server
  (run-jetty (wrap-reload #'app) {:port 8080 :join? false}))

(defn server-restart! []
  (doto server
    (.stop)
    (.start)))

(defn -main []
  (println "oi"))
