(ns irs990.core
  (:require [clj-http.client :as client]
            [clojure.data.xml :as xml])
  (:gen-class))

(defn get-xml [ url ]
  (xml/parse-str (:body (client/get url))))

(defn xml-fields-of [ element tag-name ]
  (filter #(= (:tag %) tag-name) (:content element)))

(defn xml-field-of [ element tag-name ]
  (first (xml-fields-of element tag-name)))

(defn xml-field-content-of [ element tag-name ]
  (:content (xml-field-of element tag-name)))

(defn xml-field-value-of [ element tag-name ]
  (first (xml-field-content-of element tag-name)))

(def base-url "https://s3.amazonaws.com/irs-form-990?max-keys=800&")

(defn req-bucket-listing [ marker-key ]
  (let [listing (get-xml (str base-url "marker=" marker-key))]
    {:count (Integer/valueOf (xml-field-value-of listing :MaxKeys))
     :truncated? (Boolean/valueOf (xml-field-value-of listing :IsTruncated))
     :resource-names (map #(xml-field-value-of % :Key)  (xml-fields-of listing :Contents))}))

(defn bucket-listing
  ([]
   (bucket-listing ""))
  ([ marker-key ]
   (let [listing-segment (req-bucket-listing marker-key)
         resource-names (:resource-names listing-segment)]
     (if (:truncated? listing-segment)
       (concat resource-names
               (lazy-seq (bucket-listing (last (:resource-names listing-segment)))))
       resource-names))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (let [listing (bucket-listing)]
    (doseq [ key listing ]
      (println key))
    (println [:count (count listing)]))
  (println "end run."))
