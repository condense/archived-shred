(ns shred.core-test
  (:require [clojure.test :refer :all]
            [shred.core :refer :all]
            [shred.protocols :refer [pk rel has-many?]]
            [shred.impl.simple-schema :refer [simple-schema]]))


(deftest test-analyse-join-tables
  (let [db-schema (simple-schema {} {})
        env {:db-schema db-schema}]
    (->> {:type         :join,
          :dispatch-key :a,
          :key          :a,
          :query        [:b {:c [:d]}],
          :children
                        [{:type :prop, :dispatch-key :b, :key :b}
                         {:type     :join, :dispatch-key :c, :key :c, :query [:d],
                          :children [{:type :prop, :dispatch-key :d, :key :d}]}]}
         (preprocess-ast env)
         (analyse-join-tables env))))


(deftest test-flatten-nested-joins
  (let [db-schema (simple-schema {} {})
        env {:db-schema db-schema}
        ast {:type     :join,
               :children [{:type :expr, :path [:a :a1], :expr :a6938.a1}
                          {:type     :join,
                           :children [{:type :expr, :path [:a :b1 :b2], :expr :b16939.b2}],
                           :table    :b1,
                           :on       [[:a6938.b1_Id :b16939.Id]],
                           :many?    false,
                           :alias    :b16939,
                           :path     [:a :b1]}
                          {:type  :join,
                           :children
                                  [{:type     :join,
                                    :children [{:type :expr, :path [:a :b2 :c3 :asdf], :expr :c36941.asdf}],
                                    :table    :c3,
                                    :on       [[:b26940.c3_Id :c36941.Id]],
                                    :many?    false,
                                    :alias    :c36941,
                                    :path     [:a :b2 :c3]}],
                           :table :b2,
                           :on    [[:a6938.b2_Id :b26940.Id]],
                           :many? false,
                           :alias :b26940,
                           :path  [:a :b2]}],
               :root?    true,
               :table    :a,
               :alias    :a6938,
               :path     [:a]}
        out (flatten-nested-joins env ast)]
    (is (= #{:b1 :b2 :c3}
           (ast-join-tables out))
        "Expected joins by table name")
    (is (= #{[:a :a1] [:a :b1 :b2] [:a :b2 :c3 :asdf]}
           (ast-expr-paths out))
        "Expected expressions by path")))


(deftest test-not-flatten-nested-many-join
  (let [db-schema (simple-schema
                    {} {[:Meetings :Races]        [:Races {:Id :MeetingId}]
                        [:Meetings :Jurisdiction] [:Jurisdictions {:JurisdictionId :Id}]})
        env {:db-schema db-schema}
        ast {:type  :join,
             :children
                    [{:type     :join,
                      :children [{:type :expr,
                                  :path [:Meetings :Races :RaceNumber],
                                  :expr :races6975.RaceNumber}],
                      :table    :Races,
                      :on       [[:meetings6974.Id :races6975.MeetingId]],
                      :many?    true,
                      :alias    :races6975,
                      :path     [:Meetings :Races]}
                     {:type     :join,
                      :children [{:type :expr,
                                  :path [:Meetings :Jurisdiction :Name],
                                  :expr :jurisdiction6976.Name}],
                      :table    :Jurisdictions,
                      :on       [[:meetings6974.JurisdictionId :jurisdiction6976.Id]],
                      :many?    false,
                      :alias    :jurisdiction6976,
                      :path     [:Meetings :Jurisdiction]}],
             :root? true,
             :table :Meetings,
             :alias :meetings6974,
             :path  [:Meetings]}
        out (flatten-nested-joins env ast)]

    (is (= #{:Jurisdictions}
           (ast-join-tables out))
        "Expected joins by table name")
    (is (= #{[:Meetings :Jurisdiction :Name]}
           (ast-expr-paths out))
        "Expected expressions by path")))


(deftest test-raise-stitches
  (let [db-schema (simple-schema
                    {} {[:Meetings :Races]        [:Races {:Id :MeetingId}]
                        [:Meetings :Jurisdiction] [:Jurisdictions {:JurisdictionId :Id}]})
        env {:db-schema db-schema}
        ast {:type     :join,
             :children [{:type  :join,
                         :children
                                [{:type :expr,
                                  :path [:Meetings :Races :RaceNumber],
                                  :expr :races17526.RaceNumber}],
                         :table :Races,
                         :on    [[:meetings17525.Id :races17526.MeetingId]],
                         :many? true,
                         :alias :races17526,
                         :path  [:Meetings :Races]}
                        {:type :expr,
                         :path [:Meetings :Jurisdiction :Name],
                         :expr :jurisdiction17527.Name}],
             :root?    true,
             :table    :Meetings,
             :alias    :meetings17525,
             :path     [:Meetings],
             :join     [[:Jurisdictions :jurisdiction17527]
                        [[:meetings17525.JurisdictionId :jurisdiction17527.Id]]]}
        out (raise-stitches env ast)]

    (println out)))