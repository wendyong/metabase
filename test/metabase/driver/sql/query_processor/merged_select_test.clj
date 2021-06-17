(ns metabase.driver.sql.query-processor.merged-select-test
  (:require [clojure.test :refer :all]
            [metabase.driver.sql.query-processor-test :as sql.qp.test]
            [metabase.driver.sql.query-processor.merged-select :as sql.qp.merged-select]
            [metabase.test :as mt]))

(deftest add-merged-select-test
  (mt/dataset sample-dataset
    (mt/with-everything-store
      (let [small-inner-query    (get-in (sql.qp.test/mega-query) [:query :source-query])
            expected-inner-query (:query
                                  (mt/mbql-query nil
                                    {:source-table  $$orders
                                     :sql.qp/select [{:clause &P1.products.category
                                                      :alias  "P1__CATEGORY"
                                                      :source :breakout
                                                      :field  {:source-table "P1"
                                                               :source-alias "CATEGORY"}}
                                                     {:clause &People.people.source
                                                      :alias  "People__SOURCE"
                                                      :source :breakout
                                                      :field  {:source-table "People"
                                                               :source-alias "SOURCE"}}
                                                     {:clause [:aggregation-options [:count] {:name "count"}]
                                                      :alias  "count"
                                                      :source :aggregation}]
                                     :breakout      [&P1.products.category
                                                     &People.people.source]
                                     :order-by      [[:asc &P1.products.category]
                                                     [:asc &People.people.source]]
                                     :joins         [{:strategy     :left-join
                                                      :source-table $$products
                                                      :condition    [:= $orders.product_id &P1.products.id]
                                                      :alias        "P1"}
                                                     {:strategy     :left-join
                                                      :source-table $$people
                                                      :condition    [:= $orders.user_id &People.people.id]
                                                      :alias        "People"}]}))]
        (is (= expected-inner-query
               (sql.qp.merged-select/add-merged-select :h2 small-inner-query)))
        #_(testing "mega query"
            (let [mega-query (:query (sql.qp.test/mega-query))]
              (is (= (:query
                      (mt/mbql-query nil
                        {:sql.qp/select [{:clause &P1.products.category
                                          :source :fields
                                          :alias  "P1__CATEGORY"
                                          :field  {:source-table "source"
                                                   :source-alias "P1__CATEGORY"}}
                                         {:clause &People.people.source
                                          :source :fields
                                          :alias  "People__SOURCE"
                                          :field  {:source-table "source"
                                                   :source-alias "People__SOURCE"}}
                                         {:clause [:field "count" {:base-type :type/BigInteger}]
                                          :source :fields
                                          :alias  "count"
                                          :field  {:source-table "source"
                                                   :source-alias "count"}}
                                         {:clause &Q2.products.category
                                          :source :fields
                                          :alias  "Q2__CATEGORY"
                                          :field  {:source-table "Q2"
                                                   :source-alias "P2__CATEGORY"}}
                                         {:clause [:field "avg"
                                                   {:base-type :type/Integer, :join-alias "Q2"}]
                                          :source :fields
                                          :alias  "avg"
                                          :field  {:source-table "Q2"
                                                   :source-alias "avg"}}]
                         :source-query  expected-inner-query
                         :joins         [{:strategy     :left-join
                                          :condition    [:= $products.category &Q2.products.category]
                                          :alias        "Q2"
                                          :source-query {:source-table  $$reviews
                                                         :sql.qp/select [{:clause &P2.products.category
                                                                          :source :breakout
                                                                          :alias  "P2__CATEGORY"
                                                                          :field  {:source-table "P2"
                                                                                   :source-alias "CATEGORY"}}
                                                                         {:clause [:aggregation-options
                                                                                   [:avg $reviews.rating]
                                                                                   {:name "avg"}]
                                                                          :source :aggregation
                                                                          :alias  "avg"}]
                                                         :breakout      [&P2.products.category]
                                                         :joins         [{:strategy     :left-join
                                                                          :source-table $$products
                                                                          :condition    [:= $reviews.product_id &P2.products.id]
                                                                          :alias        "P2"}]}}]
                         :limit         2}))
                     (sql.qp.merged-select/add-merged-select :h2 mega-query)))))))))

(deftest disambiguate-names-test
  (mt/with-everything-store
    (is (= (:query
            (mt/mbql-query checkins
              {:sql.qp/select [{:clause !month.date,
                                :source :breakout
                                :alias  "DATE"}
                               {:clause !day.date
                                :source :breakout
                                :alias  "DATE_2"}]
               :breakout      [!month.date !day.date]}))
           (sql.qp.merged-select/add-merged-select
            :h2
            (:query
             (mt/mbql-query checkins
               {:breakout [!month.date !day.date]})))))))
