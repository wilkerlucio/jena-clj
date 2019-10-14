(ns com.wsscode.jena-clj.core-test
  (:require [clojure.test :refer :all]
            [com.wsscode.jena-clj.core :as jena])
  (:import (org.apache.jena.datatypes.xsd XSDDatatype)))

(deftest test-clj->jena-node
  (testing "number"
    (is (= (jena/clj->jena-node 3)
           (jena/jena-typed-literal 3)))

    (is (= (jena/clj->jena-node 3.5)
           (jena/jena-typed-literal 3.5))))

  (testing "string"
    (is (= (jena/clj->jena-node "foo")
           (jena/jena-string-literal "foo"))))

  (testing "symbol"
    (is (= (jena/clj->jena-node 'sym)
           (jena/jena-edn-literal 'sym))))

  (testing "keyword"
    (is (= (jena/clj->jena-node :some/kw)
           (jena/jena-edn-literal :some/kw))))

  (testing "uuid"
    (is (= (jena/clj->jena-node #uuid"5c8facf3-12de-41fe-9215-3505821975ed")
           (jena/jena-resource "urn:uuid:5c8facf3-12de-41fe-9215-3505821975ed"))))

  (testing "string resource"
    (is (= (jena/clj->jena-node {::jena/prefixes {"foo" "expanded/"}}
             (jena/resource-value "foo:bar"))
           (jena/jena-resource "expanded/bar"))))

  (testing "kw resource"
    (is (= (jena/clj->jena-node {::jena/prefixes {"foo" "expanded/"}}
             (jena/resource-value :foo/bar))
           (jena/jena-resource "expanded/bar"))))

  (testing "implicit type literal"
    (is (= (jena/clj->jena-node {::jena/type  ::jena/type-typed-literal
                                 ::jena/value true})
           (jena/jena-typed-literal true)))
    (is (= (jena/clj->jena-node {::jena/type  ::jena/type-literal-string
                                 ::jena/value "str"})
           (jena/jena-string-literal "str"))))

  (testing "explicit type literal"
    (is (= (jena/clj->jena-node {::jena/type  ::jena/type-typed-literal2
                                 ::jena/value ["2019-10-15" XSDDatatype/XSDdate]})
           (jena/jena-typed-literal "2019-10-15" XSDDatatype/XSDdate))))

  (testing "edn encoded"
    (testing "collections"
      (is (= (jena/clj->jena-node {:foo 42})
             (jena/jena-typed-literal "{:foo 42}" "http://www.wsscode.com/wise/EdnType"))))))

(deftest test-jena-node->clj
  (testing "number"
    (is (= (jena/jena-node->clj {} (jena/jena-typed-literal 3))
           3)))

  (testing "string"
    (is (= (jena/jena-node->clj {} (jena/jena-typed-literal "String"))
           "String")))

  (testing "uuid"
    (is (= (jena/jena-node->clj {} (jena/jena-resource "urn:uuid:5c8facf3-12de-41fe-9215-3505821975ed"))
           #uuid "5c8facf3-12de-41fe-9215-3505821975ed")))

  (testing "edn encoded"
    (is (= (jena/jena-node->clj {} (jena/jena-edn-literal {:complex 42}))
           {:complex 42}))))

; TODO write generative tests

(deftest test-map->triples
  (is (= (jena/map->triples {::jena/resource-uri "http://site.com/bla"})
         []))
  (is (= (jena/map->triples {::jena/resource-uri "domain:bla"
                             "foo"               "bar"
                             "name"              "Value"
                             "number"            33})
         [[::jena/tx-add "domain:bla" "foo" "bar"]
          [::jena/tx-add "domain:bla" "name" "Value"]
          [::jena/tx-add "domain:bla" "number" 33]])))

(deftest test-prepend-prefixes
  (is (= (jena/prepend-prefixes {::jena/prefixes jena/default-prefixes}
           "sample query")
         "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\nPREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nPREFIX owl: <http://www.w3.org/2002/07/owl#>\nsample query")))

(deftest test-expand-node
  (is (= (jena/expand-resource-uri {}
           "sample")
         "sample"))
  (is (= (jena/expand-resource-uri {}
           "missing:sample")
         "missing:sample"))
  (is (= (jena/expand-resource-uri {}
           :value)
         "/value"))
  (is (= (jena/expand-resource-uri {::jena/prefixes {"demo" "http://site.com/"}}
           "demo:value")
         "http://site.com/value"))
  (is (= (jena/expand-resource-uri {::jena/prefixes {"demo" "http://site.com/"}}
           :demo/value)
         "http://site.com/value")))

(deftest test-expand-resource-statements
  (is (= (jena/expand-resource-statements [::jena/tx-add "foo" "bar" "baz"])
         [[::jena/tx-add "foo" "bar" "baz"]]))
  (is (= (jena/expand-resource-statements {::jena/resource-uri "foo"
                                           "bar"               "baz"
                                           "other"             "more"})
         [[::jena/tx-add "foo" "bar" "baz"]
          [::jena/tx-add "foo" "other" "more"]])))

(def sample-resource "http://example.com/Resource")

(defn resource-map [env resource-uri]
  (->> (jena/jena-model-resource env resource-uri)
       (jena/jena-resource-map env)))

(def demo-prefixes
  (assoc jena/default-prefixes
    "demo" "http://demo-site.com/#"))

(defn sample-env []
  (let [model (jena/jena-model)]
    {::jena/jena-model model
     ::jena/prefixes   demo-prefixes}))

(deftest test-statement-add-remove
  (let [m (sample-env)]
    (jena/tx-statements! m [{::jena/resource-uri sample-resource
                             :rdfs/label         "Example"
                             :rdfs/value         "X"}])

    (jena/tx-statement! m [::jena/tx-rem sample-resource :rdfs/label "Example"])

    (is (= (into {} (resource-map m sample-resource))
           {"http://www.w3.org/2000/01/rdf-schema#value" "X"
            ::jena/resource-uri                          sample-resource})))

  (testing "without value it should remove all statements of that property"
    (let [m (sample-env)]
      (jena/tx-statements! m [{::jena/resource-uri sample-resource
                               :rdfs/label         "Example"
                               :rdfs/value         "X"}])

      (jena/tx-statement! m [::jena/tx-rem sample-resource :rdfs/label nil])

      (is (= (into {} (resource-map m sample-resource))
             {"http://www.w3.org/2000/01/rdf-schema#value" "X"
              ::jena/resource-uri                          sample-resource})))))

(deftest test-resource->map
  (let [m (sample-env)]
    (jena/tx-statements! m
      [[::jena/tx-add sample-resource :rdfs/label "Example"]
       [::jena/tx-add sample-resource :rdfs/number-value 3]])
    (is (= (resource-map m sample-resource)
           {"http://www.w3.org/2000/01/rdf-schema#label"        "Example"
            "http://www.w3.org/2000/01/rdf-schema#number-value" 3
            ::jena/resource-uri                                 sample-resource})))

  (let [m  (sample-env)
        id (java.util.UUID/randomUUID)]
    (jena/tx-statement! m [::jena/tx-add sample-resource :rdfs/id id])
    (is (= (:rdfs/id (resource-map m sample-resource))
           id)))

  (let [m (-> (sample-env)
              (assoc ::jena/schema-config
                {"http://www.w3.org/2000/01/rdf-schema#label"
                 {::jena/cardinality ::jena/to-many}}))]
    (jena/tx-statement! m [::jena/tx-add sample-resource :rdfs/label "Example"])
    (is (= (resource-map m sample-resource)
           {"http://www.w3.org/2000/01/rdf-schema#label" ["Example"]
            ::jena/resource-uri                          sample-resource}))))

(deftest test-update-resource!
  (testing "create new entry"
    (let [m (sample-env)]
      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/label         "Example"})

      (is (= (resource-map m sample-resource)
             {::jena/resource-uri                          sample-resource
              "http://www.w3.org/2000/01/rdf-schema#label" "Example"})))

    (let [m (sample-env)]
      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/label         {:complex "data"}})

      (is (= (resource-map m sample-resource)
             {::jena/resource-uri                          sample-resource
              "http://www.w3.org/2000/01/rdf-schema#label" {:complex "data"}}))))

  (testing "update info"
    (let [m (sample-env)]
      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/label         "Example"})

      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/new-thing     "content"
         :rdfs/label         "Modified"})

      (is (= (resource-map m sample-resource)
             {"http://www.w3.org/2000/01/rdf-schema#label"     "Modified"
              "http://www.w3.org/2000/01/rdf-schema#new-thing" "content"
              :com.wsscode.wise.jena.core/resource-uri         "http://example.com/Resource"})))

    (let [m (sample-env)]
      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/number-value  1})

      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/new-thing     "content"
         :rdfs/number-value  2})

      (is (= (resource-map (assoc m ::jena/schema-config {"http://www.w3.org/2000/01/rdf-schema#number-value" {::jena/cardinality ::jena/to-many}}) sample-resource)
             {"http://www.w3.org/2000/01/rdf-schema#number-value" [2]
              "http://www.w3.org/2000/01/rdf-schema#new-thing"    "content"
              :com.wsscode.wise.jena.core/resource-uri            "http://example.com/Resource"})))

    (let [m (sample-env)]
      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/set-value     #{1 2 3}})

      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/new-thing     "content"
         :rdfs/set-value     #{3 4 5}})

      (is (= (resource-map (assoc m ::jena/schema-config {"http://www.w3.org/2000/01/rdf-schema#set-value" {::jena/cardinality ::jena/to-many}}) sample-resource)
             {"http://www.w3.org/2000/01/rdf-schema#set-value"    [#{3 4 5}]
              "http://www.w3.org/2000/01/rdf-schema#new-thing"    "content"
              :com.wsscode.wise.jena.core/resource-uri            "http://example.com/Resource"})))

    (let [m (sample-env)]
      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/label         "Example"})

      (jena/update-resource! m
        {::jena/resource-uri sample-resource
         :rdfs/new-thing     "content"})

      (is (= (resource-map m sample-resource)
             {"http://www.w3.org/2000/01/rdf-schema#label"     "Example"
              "http://www.w3.org/2000/01/rdf-schema#new-thing" "content"
              :com.wsscode.wise.jena.core/resource-uri         "http://example.com/Resource"})))))

(deftest test-query-resources
  (let [m (sample-env)]
    (jena/tx-statements! m
      [{::jena/resource-uri "http://example.com/Resource1"
        :rdf/type           (jena/resource-value :demo/A)
        :rdfs/label         "First"}
       {::jena/resource-uri "http://example.com/Resource2"
        :rdf/type           (jena/resource-value :demo/A)
        :rdfs/label         "Second"}
       {::jena/resource-uri "http://example.com/Resource3"
        :rdf/type           (jena/resource-value :demo/B)
        :rdfs/label         "Type B"}])

    (is (= (->> (jena/query-resources m "select ?s { ?s a demo:A }"
                  [::jena/resource-uri]))
           [{::jena/resource-uri "http://example.com/Resource1"}
            {::jena/resource-uri "http://example.com/Resource2"}]))))
