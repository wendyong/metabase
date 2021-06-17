(ns metabase.driver.sql.query-processor.merged-select
  "Preprocess query (all levels) to:

   Create a `:sql.qp/fields` clause that exactly matches the Fields in the `SELECT` clause we're generating, complete
  with the appropriate aliases."
  (:refer-clojure :exclude [alias])
  (:require [metabase.driver.sql.query-processor.alias :as sql.qp.alias]
            [metabase.mbql.schema :as mbql.s]
            [metabase.mbql.util :as mbql.u]
            [metabase.util.schema :as su]
            [schema.core :as s]))

(defn add-unambiguous-alias [driver {:keys [clause source], :as m}]
  (assoc m :alias (sql.qp.alias/clause-alias driver clause)))

(defn- deduplicate-aliases [rf]
  (let [unique-name-fn (mbql.u/unique-name-generator)]
    ((map (fn [info]
            (update info :alias unique-name-fn))) rf)))

(def ^:private SelectInfo
  (-> [{:clause s/Any
        :source (s/enum :fields :aggregation :breakout)
        :alias  su/NonBlankString}]
      (s/constrained (fn [infos]
                       (or (empty? infos)
                           (apply distinct? (map :alias infos))))
                     "all aliases must be unique")))

(s/defn merged-select :- SelectInfo
  [driver inner-query :- mbql.s/MBQLQuery]
  (transduce
   (comp (map (fn [k]
                (for [clause (get inner-query k)]
                  {:clause clause
                   :source k})))
         cat
         (map (partial add-unambiguous-alias driver))
         deduplicate-aliases)
   conj
   []
   [:breakout :aggregation :fields]))

(defn add-merged-select
  [driver {:keys [source-query joins], :as inner-query}]
  (let [add-merged-select* (partial add-merged-select driver)
        inner-query        (cond-> inner-query
                             source-query (update :source-query add-merged-select*)
                             (seq joins)  (update :joins (partial mapv add-merged-select*)))
        select             (merged-select driver inner-query)]
    (cond-> (dissoc inner-query :fields :aggregation)
      (seq select) (assoc :sql.qp/select select))))
