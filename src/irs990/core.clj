(ns irs990.core
  (:require [clj-http.client :as http]
            [clojure.data.xml :as xml])
  (:gen-class))

(def debug true)

(def cm (clj-http.conn-mgr/make-reusable-conn-manager {:timeout 10 :threads 1}))

(defn get-text [ url ]
  (when debug
    (println [:get url]))
  (:body (http/get url {:connection-manager cm})))

(defn get-xml [ url ]
  (xml/parse-str (get-text url)))

(defn xml-fields-of [ element tag-name ]
  (filter #(= (:tag %) tag-name) (:content element)))

(defn xml-field-of [ element tag-name ]
  (first (xml-fields-of element tag-name)))

(defn xml-field-content-of [ element tag-name ]
  (:content (xml-field-of element tag-name)))

(defn xml-field-value-of [ element tag-name ]
  (first (xml-field-content-of element tag-name)))

(def base-url "https://s3.amazonaws.com/irs-form-990/")

(defn parse-bucket-contents-entry [ element ]
  {:key (xml-field-value-of element :Key)
   :size (xml-field-value-of element :Size)})

(defn req-bucket-listing [ marker-key ]
  (let [listing (get-xml (str base-url "?max-keys=800&marker=" marker-key))]
    {:count (Integer/valueOf (xml-field-value-of listing :MaxKeys))
     :truncated? (Boolean/valueOf (xml-field-value-of listing :IsTruncated))
     :resources (map parse-bucket-contents-entry
                          (xml-fields-of listing :Contents))}))

(defn bucket-listing
  ([]
   (bucket-listing ""))
  ([ marker-key ]
   (let [listing-segment (req-bucket-listing marker-key)
         resources (:resources listing-segment)]
     (if (:truncated? listing-segment)
       (concat resources
               (lazy-seq (bucket-listing (:key (last (:resources listing-segment))))))
       resources))))

(defn bucket-file [ key ]
  (get-text (str base-url key)))

(defn spit-gzip [ filename text ]
  (with-open [fos (java.io.FileOutputStream. filename)
              writer (java.io.OutputStreamWriter. (java.util.zip.GZIPOutputStream. fos) "UTF-8")]
    (.write writer text)))

(defn ensure-directory [ directory-path ]
  (let [output-directory (java.io.File. directory-path)]
    (if (not (.isDirectory output-directory))
      (.mkdirs output-directory))))

(def ensure-directory-once (memoize ensure-directory))

(defn write-output-file [ key text ]
  (let [bin (.substring key 0 6)
        output-path (str "output/" bin "/")]
    (ensure-directory-once output-path)
    (spit-gzip (str output-path key ".gz") text)))

(defn download-file [ contents-entry ]
  (let [key (:key contents-entry )
        text (bucket-file key)]
    (write-output-file key text)))

(defn download-files [ listing ]
  (doall (pmap download-file (take 500 listing))))

(defn print-listing [ listing ]
  (doseq [ key listing ]
    (println key)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (time
   (do
     (let [action (case (or (first args) "list")
                    "list" print-listing
                    "download" download-files
                    (throw (RuntimeException. "Must be either 'list' or 'download'")))
           listing (bucket-listing)]
       (action listing))
     (shutdown-agents)
     (println "end run."))))
