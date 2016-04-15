(ns shred.core
  (:require [schema.core :as s]
            [om.next.impl.parser :refer [expr->ast]]
            [clojure.walk :refer [prewalk postwalk]]
            [clojure.pprint :refer [pprint]]
            [clojure.string :refer [lower-case]]
            [clojure.test :refer [is]]
            [shred.protocols :refer [rel has-many? pk]]))

; TODO: #shred
;  - Handling params:
;       Should error until handled.
;       Perhaps don't split if params are present since they are select query modifiers.
;  - Not currently adding the joining keys necesary to stitch up data.

; TODO: #query
;  - Create HoneySQL from shredded query ast.
;  - Decide if it's better to pass IDs between queries or use repeated subqueries each time.

; TODO: #stitch
;  - Stitch data back together after running queries.




(defn preprocess-ast
  [env ast]
  (assoc ast :root? true))

(defn genalias
  "Generate a unique but meaningful keyword for use as an alias."
  [prefix]
  (-> prefix name lower-case gensym keyword))

(defn col-with-alias
  [table-alias column-name]
  {:pre [(is (keyword? table-alias))
         (is (keyword? column-name))]}
  (keyword (str (name table-alias) "." (name column-name))))

(defmulti analyse-join-child
  "
  * Annotate props with :type :path and :expr
  * Annotate joins with :table :on :many? :alias :path
  "
  (fn [db-schema key alias path ast] (:type ast)))

(defmethod analyse-join-child :default [db-schema key alias path ast] ast)

(def ExpressionAST
  {:type s/Keyword
   :path [s/Keyword]
   :expr s/Any
   s/Any s/Any})

(defmethod analyse-join-child :prop
  [db-schema from-table from-alias path {:keys [key] :as ast}]
  {:post [(s/validate ExpressionAST %)]}
  (-> ast
      (dissoc :key :dispatch-key)
      (assoc
        :type :expr
        :path (conj path key)
        :expr (col-with-alias from-alias key))))

(def RelationAST
  {:table s/Any
   :on    s/Any
   :many? s/Bool
   :alias s/Keyword
   :path  s/Any
   s/Any  s/Any})

(defmethod analyse-join-child :join
  [db-schema from-table from-alias path {:keys [key] :as ast}]
  {:post [(s/validate RelationAST %)]}
  (if-let [{:keys [to on]} (rel db-schema from-table key)]
    (let [to-alias (genalias key)
          on (into [] (for [[from-col to-col] on]
                        [(col-with-alias from-alias from-col)
                         (col-with-alias to-alias to-col)]))]
      (-> ast
          (dissoc :key :dispatch-key :query)
          (assoc :table to
                 :on on
                 :many? (has-many? db-schema from-table key)
                 :alias to-alias
                 :path (conj path key))))
    (throw (ex-info (str "Relation not found from table " from-table " with key " key)
                    {:from-table from-table :key key}))))

(defn analyse-join-tables
  "
  Analyse pull ast and annotate with :table :alias :path
  "
  [{:keys [db-schema]}
   {:keys [key type root? table alias path] :as ast}]

  (cond root?
        (let [alias (genalias key)
              path [key]
              child-fn (partial analyse-join-child db-schema key alias path)]
          (-> ast
              (dissoc :key :dispatch-key :query)
              (assoc :table key
                     :alias alias
                     :path path)
              (update :children #(mapv child-fn %))))

        (= :join type)
        (let [child-fn (partial analyse-join-child db-schema table alias path)]
          (-> ast
              (assoc :table table
                     :alias alias)
              (update :children #(mapv child-fn %))))

        :else
        ast))

(defmulti -flatten-nested-joins
  "
  Identify join children:
   * Leave many? joins
   * Absorb join :children into :children
   * Add :join expression for each absorbed join
  "
  (fn [env ast] (:type ast)))

(defmethod -flatten-nested-joins :default [env ast] ast)

(defmethod -flatten-nested-joins :join
  [env ast]
  (let [{:keys [children]} ast
        {:keys [join]} (group-by :type children)]
    (reduce
      (fn lift-join
        [ast join-ast]
        (let [{:keys [table on alias children join many? stitch]} join-ast
              {:keys [expr]} (group-by :type children)]
          (if many?
            ast
            (-> ast
                (update :children (partial remove #{join-ast}))
                (update :children concat expr)
                (update :join concat join [[table alias] on])))))
      ast join)))

(defn ast-join-tables [ast]
  (->> ast :join (partition 2) (map ffirst) set))

(defn ast-expr-paths [ast]
  (->> ast :children (filter #(-> % :type (= :expr))) (map :path) set))

(defn flatten-nested-joins
  "docstring"
  [env ast]
  (postwalk (partial -flatten-nested-joins env) ast))

(defmulti -raise-stitches
  "Walk up the tree picking off nested join queries.
   Each query is added to state atom."
  (fn [env ast] (:type ast)))

(defmethod -raise-stitches :default [env ast] ast)

(defmethod -raise-stitches :join
  [{:keys [state]} {:keys [root? many?] :as ast}]
  (let [ast (update ast :children #(remove nil? %))]
    (cond
      root? (do (swap! state conj ast) nil)
      many? (do (swap! state conj ast) nil)
      :else ast)))

(defn raise-stitches
  "Return list of simple queries."
  [env ast]
  (let [state (atom [])
        env' (assoc env :state state)]
    (postwalk (partial -raise-stitches env') ast)
    @state))

(defn build-queries
  "Take db schema and query expression.
   Return the query plan which can be executed as a series of regular SQL queries whose results
   can be stitched back up to produce a nested query result set."
  [db-schema query]
  (let [env {:db-schema db-schema}
        ast (expr->ast query)]
    (->> ast
         (preprocess-ast env)
         (prewalk (partial analyse-join-tables env))
         (flatten-nested-joins env)
         (raise-stitches env))))

(do

  (require 'shred.impl.simple-schema)

  (let [db-schema (shred.impl.simple-schema/simple-schema
                    {:User :UserId}
                    {
                     ;[:Races :Meeting]    [:Meetings {:MeetingId :Id}]
                     ;[:Races :Analyst]    [:User {:AnalystId :UserId}]
                     ;[:Races :Supervisor] [:User {:SupervisorId :UserId}]
                     ;[:Meetings :Races]   [:Races {:Id :MeetingId}]
                     ;[:Meetings :Venue]   [:Venues {:VenueId :Id}]
                     ;[:Venues :Meetings]  [:Meetings {:Id :VenueId}]
                     })
        query {:Venues [{:Meetings [:MeetDate]}]}]
    (-> (build-queries db-schema query)
        (pprint))))