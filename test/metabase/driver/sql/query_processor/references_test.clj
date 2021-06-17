(ns metabase.driver.sql.query-processor.references-test
  (:require [clojure.test :refer :all]
            [metabase.driver.sql.query-processor-test :as sql.qp.test]
            [metabase.driver.sql.query-processor.merged-select :as merged-select]
            [metabase.driver.sql.query-processor.references :as references]
            [metabase.test :as mt]))

(defn- less-stuff [x]
  (walk/postwalk
   (fn [form]
     (if (map? form)
       (dissoc form :strategy :condition :fields :breakout :sql.qp/select :limit :order-by :alias)
       form))
   x))

(deftest add-references-test
  (mt/dataset sample-dataset
    (mt/with-everything-store
      (let [small-inner-query (get-in (sql.qp.test/mega-query) [:query :source-query])]
        (is (= (mt/$ids
                 {&P1.products.category
                  {:source-alias "CATEGORY", :source-table "P1"}

                  &People.people.source
                  {:source-table "People", :source-alias "SOURCE"}})
               (->> small-inner-query
                    (merged-select/add-merged-select :h2)
                    (references/add-references :h2)
                    :sql.qp/references)))
        (testing "mega query"
          (let [mega-query (:query (sql.qp.test/mega-query))]
            (is (= (mt/$ids
                     {:source-query {:source-table      $$orders
                                     :joins             [{:source-table $$products
                                                          :sql.qp/references {}}
                                                         {:source-table $$people}]
                                     :sql.qp/references {&P1.products.category
                                                         {:source-table "P1", :source-alias "CATEGORY"}

                                                         &People.people.source
                                                         {:source-table "People", :source-alias "SOURCE"}}},
                      :joins             [{:source-query
                                           {:source-table $$reviews
                                            :joins        [{:source-table $$products}]}}]
                      :sql.qp/references {&P1.products.category
                                          {:source-alias "P1__CATEGORY", :source-table "source"}

                                          &People.people.source
                                          {:source-alias "People__SOURCE", :source-table "source"}

                                          [:field "count" {:base-type :type/BigInteger}]
                                          {:source-alias "count", :source-table "source"}

                                          &Q2.products.category
                                          {:source-alias "P2__CATEGORY", :source-table "Q2"}

                                          [:field "avg" {:base-type :type/Integer, :join-alias "Q2"}]
                                          {:source-alias "avg", :source-table "Q2"}}})
                   (->> mega-query
                        (merged-select/add-merged-select :h2)
                        (references/add-references :h2)
                        less-stuff)))))))))
