(ns shred.impl.simple-schema
  "
  In this code we present a simple implementation of the IDBSchema protocol which might serves
  as a reference and aid to testing.

  Simple schema implementation makes some assumptions:
  1. Use pks to lookup table keys.
  2. Assume the table's key is :Id if not present in pks.
     e.g. (pk (simple-schema {} {}) :a) will return :Id
  3. Use rels to lookup relation details.
  4. Assume many-to-one relation exists between tables if not present in rels
     e.g. (rel (simple-schema {} {}) :a :b) will guess that table :a has a foreign key :b_id

  "
  (:require [shred.protocols :refer [pk rel has-many?]]
            [schema.core :as s])
  (:import (shred.protocols IDBSchema)))

(def TableKeyword s/Keyword)
(def ColumnKeyword s/Keyword)
(def RelationKeyword s/Keyword)

(def PrimaryKeys
  "Maps tables to their primary key column.  Doesn't support composite keys."
  {TableKeyword ColumnKeyword})

(defn Tuple [& specs]
  (let [one-fn (fn [spec idx]
                 (s/one spec (str "TupleIdx" idx)))]
    (mapv one-fn specs (range))))

(def Relations
  "Maps between a source table/relation and the destination table.
   The relation maps source columns to destingation columns for query generation."
  {(Tuple TableKeyword RelationKeyword)
   (Tuple TableKeyword {ColumnKeyword ColumnKeyword})})

(defrecord SimpleSchema [pks rels]

  IDBSchema
  (-pk [_ table-name]
    (get pks table-name :Id))

  (-rel [schema from-table rel-name]
    (if-let [[to-table field-mapping] (get rels [from-table rel-name])]
      {:from from-table
       :to   to-table
       :on   field-mapping}
      {:from from-table
       :to   rel-name
       :on   {(keyword (str (name rel-name) "_Id"))
              (pk schema rel-name)}}))

  (-has-many?
    [db-schema table-name rel-name]
    (if-let [{:keys [on]} (-rel db-schema table-name rel-name)]
      (not= [(-pk db-schema table-name)] (vals on)))))

(defn simple-schema
  "Build a simple schema based on keys and relations"
  [pks rels]
  {:pre [(s/validate PrimaryKeys pks)
         (s/validate Relations rels)]
   :post [(satisfies? shred.protocols/IDBSchema %)]}
  (map->SimpleSchema {:pks pks :rels rels}))