(ns shred.query
  (:require [clojure.set :refer [intersection union]]
            [schema.core :as s]
            [clojure.string :as str]))

(defmacro spy [form]
  `(let [form# ~form]
     (println ~(str form) "=>" form#)
     form#))

(def Join
  {:table s/Keyword
   :on    {s/Keyword s/Keyword}
   :as    s/Keyword})

(def Select
  {:select #{s/Keyword}
   :from   s/Keyword
   :as     s/Keyword
   :join   [Join]})

(def join? (s/validator Join))
(def select? (s/validator Select))

(defn as-cols [as cols]
  (set (map #(keyword (name as) (name %)) cols)))

(defn genkey [prefix]
  (-> prefix name gensym keyword))

(defn select
  [[table as] cols]
  (let [as (or as (genkey table))]
    {:select (as-cols as cols)
     :from   table
     :as     as
     :join   []}))

(defn cols [xrel]
  {:pre [(select? xrel)]}
  (:select xrel))

(defn table [xrel]
  {:pre [(or (select? xrel) (join? xrel))]}
  (:from xrel))

(defn as [xrel]
  {:pre [(or (select? xrel) (join? xrel))]}
  (:as xrel))

(defn join [xrel [yrel as] km]
  {:pre [(select? xrel) (select? yrel)]}
  (let [as (or as (genkey (table yrel)))
        km (zipmap (keys km) (as-cols as (vals km)))]
    (-> xrel
        (update :select union (as-cols as (cols yrel)))
        (update :join conj {:table (table yrel)
                            :on    km
                            :as    as}))))

(defn project [xrel ks]
  {:pre [(select? xrel)]}
  (update xrel :select intersection (set ks)))

; extend
; restrict
; summarise

(defn sql [xrel]
  {:pre [(select? xrel)]}
  (let [dot (fn [k] (str (namespace k) "." (name k)))
        {:keys [select from as join]} xrel]
    (str "SELECT " (str/join ", " (map dot select))
         "\n"
         "  FROM " (name from) " AS " (name as)
         (str/join " " (for [{:keys [table on as]} join]
                         (str "\n"
                              "  JOIN " (name table)
                              " AS " (name as)
                              " ON ( "
                              (str/join " AND " (map (fn [[x y]] (str (dot x) "=" (dot y))) on))
                              " ) "))))))

(comment

  (def db-cols
    {:Meetings #{:Meetings/Id :Meetings/MeetDate :Meetings/VenueId}
     :Races    #{:Races/Id :Races/StartTime :Races/MeetingId}
     :Venues   #{:Venues/Id :Venues/Name}})

  (defn test-queryset []
    (let [Races (select [:Races :R] (:Races db-cols))
          Meetings (select [:Meetings :M] (:Meetings db-cols))
          Venues (select [:Venues :V] (:Venues db-cols))]
      (print
        (-> Races
            (join [Meetings :M] {:R/MeetingId :M/Id})
            (join [Venues :V] {:M/VenueId :V/Id})
            (join [Meetings :M2] {:M2/Id :R/MeetingId})
            (project [:M/MeetDate :V/Name])
            sql))))

  (test-queryset))
