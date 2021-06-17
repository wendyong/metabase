(ns metabase.driver.sql.query-processor.subselect
  "Tools for converting nesting the MBQL query a level deeper when subselects are needed, e.g. for expressions or
  cumulative aggregations."
  (:require [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.middleware.annotate :as annotate]))

(defn expressions-subselectify
  [inner-query]
  (let [subselect (-> inner-query
                      (select-keys [:joins :source-table :source-query :source-metadata :expressions])
                      (assoc :fields (-> (mbql.u/match (dissoc inner-query :source-query :joins)
                                           ;; remove the bucketing/binning operations from the source query -- we'll
                                           ;; do that at the parent level
                                           [:field id-or-name opts]
                                           [:field id-or-name (dissoc opts :temporal-unit :binning)]
                                           :expression
                                           &match)
                                         distinct)))]
    (-> (mbql.u/replace inner-query
          [:expression expression-name]
          [:field expression-name {:base-type (:base_type (annotate/infer-expression-type &match))}]
          ;; the outer select should not cast as the cast happens in the inner select
          [:field (field-id :guard int?) field-info]
          [:field field-id (assoc field-info ::outer-select true)])
        (dissoc :source-table :joins :expressions :source-metadata)
        (assoc :source-query subselect))))

#_(defn- add-cumulative-aggregation-info [inner-query]
  (let [cumulative-ag-counter (atom 0)]
    (mbql.u/replace-in inner-query [:aggregation]
      [:aggregation-options [(ag-type :guard #{:cum-sum :cum-count}) expr] (options :guard (complement ::cumulative?))]
      [:aggregation-options [ag-type expr] (assoc options
                                                  ::cumulative?   true
                                                  ::original-name (:name options)
                                                  :name           (format "cumulative__%s__%d"
                                                                          (str/replace (name ag-type) #"cum-" "")
                                                                          (swap! cumulative-ag-counter inc)))])))

(defn- rewrite-cumulative-ags-subselect [inner-query]
  (mbql.u/replace-in inner-query [:aggregation]
    [:cum-sum expr]
    [:sum expr]

    [:cum-count expr]
    [:sum expr]))

(defn- rewrite-cumulative-ags-top-level [inner-query]
  (map-indexed
   (fn [i ag]
     (mbql.u/replace ag
       [:cum-sum expr] [:cum-sum [:field "sum" {:base-type :type/Number}]]))
   (:aggregation inner-query)))

(defn- cumulative-aggregation-subselectify [inner-query]
  (let [#_inner-query #_(add-cumulative-aggregation-info inner-query)
        subselect   (rewrite-cumulative-ags-subselect inner-query)]
    (merge
     (when-let [breakouts (not-empty (:breakout inner-query))]
       {:metabase.sql.qp/subselect-breakouts breakouts})
     (when-let [fields (not-empty (:fields inner-query))]
       {:fields fields}))
    {:aggregation  (rewrite-cumulative-ags-top-level inner-query)
     :source-query subselect}))

(defn has-top-level-cumulative-aggregation? [{:keys [aggregation]}]
  (mbql.u/match-one aggregation #{:cum-sum :cum-count}))

(defn subselectify
  "Create subselect nested queries as needed if the top-level query contains expressions or cumulative aggregations."
  [{:keys [expressions], :as inner-query}]
  (cond-> inner-query
    (has-top-level-cumulative-aggregation? inner-query) cumulative-aggregation-subselectify
    (seq expressions)                                   expressions-subselectify))
