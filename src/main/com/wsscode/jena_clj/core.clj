(ns com.wsscode.jena-clj.core
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.string :as str]
            [com.wsscode.misc :as misc]
            [com.wsscode.pathom.core :as p]
            [ghostwheel.core :refer [>defn >fdef => | <- ?]]
            [potemkin.collections :refer [def-map-type]]
            [taoensso.timbre :as log])
  (:import (io.bdrc.jena.sttl STTLWriter)
           (java.io InputStream OutputStream Writer)
           (org.apache.jena.datatypes TypeMapper RDFDatatype)
           (org.apache.jena.enhanced EnhNode)
           (org.apache.jena.query ResultSetFormatter QueryExecutionFactory DatasetFactory ParameterizedSparqlString QueryFactory Dataset Query QuerySolution QuerySolutionMap ReadWrite)
           (org.apache.jena.rdf.model ResourceFactory ModelFactory Model Statement RDFNode Resource Property Literal)
           (org.apache.jena.reasoner ReasonerRegistry)
           (org.apache.jena.riot RDFDataMgr RDFParser RDFLanguages)
           (org.apache.jena.shared PrefixMapping PrefixMapping$Factory)
           (org.apache.jena.sparql.serializer SerializationContext)
           (org.apache.jena.sparql.util FmtUtils)
           (org.apache.jena.tdb TDBFactory)
           (org.apache.jena.update UpdateAction)))

(declare jena-model expand-resource-uri resource-object-value jena-node->clj jena-property)

; region custom names

(def edn-type-uri "http://www.wsscode.com/wise/EdnType")

; endregion

; region class extensions

(def-map-type ResourceMap [env ^Resource res]
  (get [_ k default-value]
    (if (= k ::resource-uri)
      (.getURI res)
      (let [{::keys [schema-config]} env
            puri        (expand-resource-uri env k)
            cardinality (get-in schema-config [puri ::cardinality] ::to-one)]
        (case cardinality
          ::to-one
          (jena-node->clj env
            (some-> res (.getProperty (jena-property puri)) (.getObject)))

          ::to-many
          (->> (.listProperties res (jena-property puri))
               (.toList)
               (mapv #(jena-node->clj env (.getObject %))))

          (do
            (log/warn "Invalid cardinality for property" :property puri :cardinality cardinality)
            res)))))
  (assoc [this k v]
    this)
  (dissoc [this k]
    this)
  (keys [_]
    (conj
      (->> res
           (.listProperties)
           (.toList)
           (into #{} (map #(str (.getPredicate %)))))
      ::resource-uri))
  (meta [_]
    nil)
  (with-meta [this mta]
    this)

  clojure.lang.Associative
  (containsKey [this k]
    (or (= k ::resource-uri)
        (contains? (.keySet this) (expand-resource-uri env k)))))

(defn jena-resource-map [env ^Resource res] (ResourceMap. env res))

; endregion

; region specs

(s/def ::input-stream #(instance? InputStream %))
(s/def ::output-stream #(instance? OutputStream %))
(s/def ::writer #(instance? Writer %))
(s/def ::output
  (s/or :stream ::output-stream
        :writer ::writer))

(s/def ::jena-dataset #(instance? Dataset %))
(s/def ::jena-literal #(instance? Literal %))
(s/def ::jena-model (s/with-gen #(instance? Model %) #(gen/return (jena-model))))
(s/def ::jena-rdf-node #(instance? RDFNode %))
(s/def ::jena-prefix-mapping (s/with-gen #(instance? PrefixMapping %) #(gen/return (PrefixMapping$Factory/create))))
(s/def ::jena-property #(instance? Property %))
(s/def ::jena-query #(instance? Query %))
(s/def ::jena-query-solution-map #(instance? QuerySolutionMap %))
(s/def ::jena-rdf-data-type #(instance? RDFDatatype %))
(s/def ::jena-resource #(instance? Resource %))
(s/def ::jena-serialization-context #(instance? SerializationContext %))
(s/def ::jena-statement #(instance? Statement %))
(s/def ::jena-value-node #(instance? EnhNode %))

(s/def ::resource-uri-str string?)
(s/def ::resource-uri (s/or :str ::resource-uri-str :kw keyword?))
(s/def ::property ::resource-uri)

(s/def ::map-resource
  (s/and
    (s/keys :req [::resource-uri])
    (s/map-of
      (s/or :id #{::resource-uri} :property ::property)
      any?)))

(s/def ::prefix (s/tuple string? string?))
(s/def ::prefixes (s/map-of string? string?))
(s/def ::bindings (s/map-of string? any?))
(s/def ::triple (s/tuple ::resource-uri ::property any?))
(s/def ::tx-op #{::tx-add ::tx-rem})
(s/def ::tx-triple (s/tuple ::tx-op ::resource-uri ::property any?))
(s/def ::triples (s/coll-of ::triple))
(s/def ::type #{::type-resource ::type-typed-literal ::type-typed-literal2 ::type-literal-string})
(s/def ::query-string string?)
(s/def ::value any?)
(s/def ::resource-value (s/keys :req [::type ::value]))
(s/def ::result-map (s/keys :req [::headers ::rows]))
(s/def ::alias qualified-keyword?)
(s/def ::rdf-data-type (s/or :str string? :data-type ::jena-rdf-data-type))

(s/def ::triple-or-res (s/or :triple ::tx-triple :res ::map-resource))

(s/def ::value-node-input
  (s/or :typed ::resource-value
        :direct any?))

(s/def ::cardinality #{::to-one ::to-many})

(s/def ::schema-config
  (s/map-of ::resource-uri-str (s/keys :opt [::cardinality])))

(s/def ::wrapped-resource #(instance? ResourceMap %))

; endregion

(def default-prefixes
  {"xsd"  "http://www.w3.org/2001/XMLSchema#"
   "rdf"  "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "rdfs" "http://www.w3.org/2000/01/rdf-schema#"
   "owl"  "http://www.w3.org/2002/07/owl#"})

; region jena wrappers

(>defn ^Model jena-model [] [=> ::jena-model]
  (ModelFactory/createDefaultModel))

(>defn ^Resource jena-model-resource [{::keys [jena-model] :as env} resource-uri]
  [(s/keys :req [::jena-model]) ::resource-uri => ::jena-resource]
  (.getResource jena-model (expand-resource-uri env resource-uri)))

(>defn ^Model jena-model-remove-all
  ([{::keys [^Model jena-model]}]
   [(s/keys :req [::jena-model]) => ::jena-model]
   (.removeAll jena-model))
  ([{::keys [^Model jena-model]} ^Resource resource ^Property property ^RDFNode node]
   [(s/keys :req [::jena-model])
    (s/nilable ::jena-resource)
    (s/nilable ::jena-property)
    (s/nilable ::jena-rdf-node)
    => ::jena-model]
   (.removeAll jena-model resource property node)))

(>defn ^Resource jena-resource [uri]
  [::resource-uri => ::jena-resource]
  (ResourceFactory/createResource uri))

(>defn ^Property jena-property [uri]
  [::property => ::jena-property]
  (ResourceFactory/createProperty uri))

(>defn ^RDFDatatype get-safe-type [n]
  [string? => ::jena-rdf-data-type]
  (-> (TypeMapper/getInstance) (.getSafeTypeByName n)))

(>defn ^Literal jena-typed-literal
  ([s]
   [any? => ::jena-literal]
   (ResourceFactory/createTypedLiteral s))
  ([s t]
   [any? ::rdf-data-type => ::jena-literal]
   (let [t' (if (string? t) (get-safe-type t) t)]
     (ResourceFactory/createTypedLiteral s t'))))

(>defn ^Literal jena-edn-literal
  [x]
  [any? => ::jena-literal]
  (jena-typed-literal (pr-str x) edn-type-uri))

(>defn ^SerializationContext jena-serialization-context [^PrefixMapping x]
  [::jena-prefix-mapping => ::jena-serialization-context]
  (SerializationContext. x))

(>defn ^Literal jena-string-literal [s]
  [string? => ::jena-literal]
  (ResourceFactory/createStringLiteral s))

(>defn ^Statement jena-statement [[subject property object]]
  [(s/tuple ::jena-resource ::jena-resource ::jena-rdf-node)
   => ::jena-statement]
  (ResourceFactory/createStatement subject property object))

(>defn model-prefixes
  [^Model model]
  [::jena-model => ::prefixes]
  (into {} (.getNsPrefixMap model)))

(>defn ^Dataset jena-dataset [^Model model]
  [::jena-model => ::jena-dataset]
  (doto (DatasetFactory/create)
    (.setDefaultModel model)))

(>defn ^PrefixMapping jena-prefix-mapping [prefixes]
  [(s/nilable ::prefixes) => ::jena-prefix-mapping]
  (let [pm (PrefixMapping$Factory/create)]
    (doseq [[k v] prefixes]
      (.setNsPrefix pm k v))
    pm))

(>defn ^Model jena-dataset-model [^Dataset dataset]
  [::jena-dataset => ::jena-model]
  (.getDefaultModel dataset))

(>defn jena-dataset-commit! [^Dataset dataset]
  [::jena-dataset => nil?]
  (.commit dataset))

(>defn jena-dataset-abort! [^Dataset dataset]
  [::jena-dataset => nil?]
  (.abort dataset))

(>defn string-for-rdf-node [^RDFNode node ^SerializationContext context]
  [::jena-rdf-node ::jena-serialization-context => string?]
  (FmtUtils/stringForRDFNode node context))

(>defn jena-uri-resource? [^RDFNode o]
  [::jena-rdf-node => boolean?]
  (and (.isResource o)
       (str/starts-with? (.toString o) "urn:uuid:")))

(def special-handled-types
  {edn-type-uri
   (fn [x]
     (read-string x))})

(>defn jena-node->clj [{::keys [prefixes] :as env} ^RDFNode x]
  [(s/keys :opt [::prefixes ::jena-serialization-context]) ::jena-rdf-node
   => any?]
  (cond
    (.isLiteral x)
    (let [x        ^Literal x
          type-uri (.getDatatypeURI x)]
      (if-let [[_ f] (find special-handled-types type-uri)]
        (f (.getLexicalForm x))
        (.getValue ^Literal x)))

    (jena-uri-resource? x)
    (let [[_ uuid] (re-find #"^urn:uuid:(.+)" (.toString x))]
      (java.util.UUID/fromString uuid))

    :else
    (let [context (or (::jena-serialization-context env)
                      (jena-serialization-context (jena-prefix-mapping prefixes)))]
      (string-for-rdf-node x context))))

; endregion

; region sparql query helpers

(>defn prefix->sparql-header [[alias uri]]
  [::prefix => string?]
  (str "PREFIX " alias ": <" uri ">"))

(>defn sparql-prefixes
  "Given a prefix map, returns a string with those prefix definitions to use
  in a SparQL query header."
  [prefixes]
  [(s/nilable ::prefixes) => string?]
  (str/join "\n" (mapv prefix->sparql-header prefixes)))

(>defn ^String prepend-prefixes
  "Generate SparQL prefixes and prepend into a given query string."
  [{::keys [prefixes]} qs]
  [(s/keys :opt [::prefixes]) ::query-string => ::query-string]
  (str (sparql-prefixes prefixes) "\n" qs))

(>defn update-model!
  "Run a SparQL UPDATE query to change data in a model."
  [{::keys [^Model jena-model prefixes] :as env} ^String qs]
  [(s/keys :req [::jena-model] :opt [::prefixes]) ::query-string => nil?]
  (let [env' (assoc env ::prefixes (or prefixes (model-prefixes jena-model)))]
    (UpdateAction/parseExecute (prepend-prefixes env' qs) jena-model)))

; endregion

; region sparql querying

(>defn ^Query create-query
  ([qs]
   [string? => ::jena-query]
   (create-query qs {}))
  ([qs prefixes]
   [string? ::prefixes => ::jena-query]
   (let [pss (doto (ParameterizedSparqlString.) (.setCommandText qs))
         _   (doseq [[k v] (merge default-prefixes prefixes)] (.setNsPrefix pss k v))]
     (QueryFactory/create (.toString pss)))))

(>defn ^QuerySolutionMap query-bindings [bindings]
  [(s/nilable map?) => ::jena-query-solution-map]
  (let [binding (QuerySolutionMap.)]
    (doseq [[k v] bindings]
      (.add binding k v))
    binding))

(>defn query-resources
  "Run a query, the query must return just the list of entities, those will be wrapped
  with ResourceMap, making then work in a clojure map-like fashion."
  ([{::keys [^Model jena-model bindings prefixes] :as env} qs]
   [map? ::query-string => (s/coll-of ::wrapped-resource)]
   (let [ds         (jena-dataset jena-model)
         query      (create-query qs (merge (model-prefixes jena-model) prefixes))
         qexec      (QueryExecutionFactory/create query ds (query-bindings bindings))
         result-set (.execSelect qexec)]
     (->> (iterator-seq result-set)
          (map (fn [^QuerySolution solution]
                 (let [vn (first (iterator-seq (.varNames solution)))]
                   (jena-resource-map env (.getResource solution vn))))))))
  ([env qs selection]
   [map? ::query-string vector? => (s/coll-of map?)]
   (map #(p/map-select % selection) (query-resources env qs))))

(>defn query-model-map
  [{::keys [^Model jena-model bindings prefixes]} qs]
  [(s/keys :req [::jena-model] :opt [::prefixes ::bindings]) ::query-string
   => ::result-map]
  (let [ds         (jena-dataset jena-model)
        query      (create-query qs (merge (model-prefixes jena-model) prefixes))
        qexec      (QueryExecutionFactory/create query ds (query-bindings bindings))
        result-set (.execSelect qexec)
        results    (iterator-seq result-set)
        context    (SerializationContext. query)]
    {::headers
     (into [] (.getResultVars result-set))

     ::rows
     (->> results
          (mapv (fn [^QuerySolution solution]
                  (reduce
                    (fn [m vn]
                      (let [v (string-for-rdf-node (.get solution vn) context)]
                        (assoc m vn v)))
                    {}
                    (iterator-seq (.varNames solution))))))}))

(>defn query-model [{::keys [^Model jena-model prefixes]} qs]
  [(s/keys :req [::jena-model] :opt [::prefixes]) ::query-string => nil?]
  (let [ds    (jena-dataset jena-model)
        query (create-query qs (merge (model-prefixes jena-model) prefixes))]
    (with-open [qexec (QueryExecutionFactory/create query ds)]
      (ResultSetFormatter/out System/out (.execSelect qexec) query))
    nil))

; endregion

; region model helpers

(>defn load-model
  ([source]
   [(s/or :path string?
          :stream ::input-stream)
    => ::jena-model]
   (cond
     (string? source)
     (RDFDataMgr/loadModel source)

     (instance? InputStream source)
     (let [model (jena-model)]
       (-> (RDFParser/create)
           (.source source)
           (.lang RDFLanguages/TTL)
           (.parse model))
       model))))

(>defn ^Model merge-models [models]
  [(s/coll-of ::jena-model) => ::jena-model]
  (let [merged (jena-model)]
    (doseq [^Model m models] (.add merged m))
    merged))

; endregion

; region jena node helpers

(>defn expand-resource-uri [{::keys [prefixes]} resource]
  [(s/keys :opt [::prefixes]) ::resource-uri
   => (s/nilable ::resource-uri-str)]
  (cond
    (string? resource)
    (if-let [[_ prefix rest] (re-find #"^([^:]+):(.+)" resource)]
      (if-let [p (get prefixes prefix)]
        (str p rest)
        resource)
      resource)

    (keyword? resource)
    (if-let [p (get prefixes (namespace resource))]
      (str p (name resource))
      (str (namespace resource) "/" (name resource)))

    :else
    (do
      (log/warn "Expand resource called with something that's not a string or a keyword." :resource resource)
      nil)))

(>defn resource-uri-key [env resource]
  [map? ::resource-uri
   => (s/nilable (s/or :uri-label #{::resource-uri}
                       :str ::resource-uri-str))]
  (if (= ::resource-uri resource)
    resource
    (expand-resource-uri env resource)))

(>defn map->triples [{::keys [resource-uri] :as data}]
  [::map-resource => (s/coll-of ::tx-triple)]
  (let [data' (dissoc data ::resource-uri)]
    (map #(vector ::tx-add resource-uri (first %) (second %)) data')))

(>defn expand-resource-statements [t]
  [::triple-or-res => (s/coll-of ::tx-triple)]
  (cond
    (vector? t) [t]
    (map? t) (map->triples t)
    :else []))

; endregion

; region statements

(>defn resource-value [x]
  [::resource-uri => ::resource-value]
  {::type  ::type-resource
   ::value x})

(>defn literal-value [x]
  [any? => ::resource-value]
  {::type  ::type-typed-literal
   ::value x})

(>defn clj->jena-node
  ([x]
   [::value-node-input => ::jena-value-node]
   (clj->jena-node {} x))
  ([env x]
   [map? ::value-node-input => ::jena-value-node]
   (if-let [type (get x ::type)]
     (let [v (::value x)]
       (case type
         ::type-resource
         (jena-resource (expand-resource-uri env v))

         ::type-typed-literal
         (jena-typed-literal v)

         ::type-typed-literal2
         (jena-typed-literal (first v) (second v))

         ::type-literal-string
         (jena-string-literal v)))
     (cond
       (uuid? x)
       (jena-resource (str "urn:uuid:" x))

       (or (symbol? x) (keyword? x) (coll? x))
       (jena-edn-literal x)

       :else
       (jena-typed-literal x)))))

(>defn ^Statement triple->statement
  "Create a jena statement from a triple."
  [env [s p o]]
  [(s/keys :opt [::prefixes]) ::triple => ::jena-statement]
  (jena-statement [(jena-resource (expand-resource-uri env s))
                   (jena-property (expand-resource-uri env p))
                   (clj->jena-node env o)]))

(>defn equal-node-value? [^RDFNode o1 ^RDFNode o2]
  [::jena-rdf-node ::jena-rdf-node => boolean?]
  (if (and (.isLiteral o1)
           (.isLiteral o2))
    (.sameValueAs ^Literal o1 ^Literal o2)
    (= o1 o2)))

(>defn ^Model tx-statement!
  ([{::keys [^Model jena-model] :as env} [tx s p o]]
   [(s/keys :req [::jena-model] :opt [::prefixes]) ::tx-triple
    => (s/keys :req [::jena-model])]
   (let [t [s p o]]
     (case tx
       ::tx-add
       (when o (.add jena-model (triple->statement env t)))

       ::tx-rem
       (let [s' (jena-model-resource env s)
             p' (jena-property (expand-resource-uri env p))]
         (if o
           (.removeAll jena-model s' p' (clj->jena-node env o))
           (.removeAll jena-model s' p' nil)))))
   env))

(>defn ^Model tx-statements!
  "Load triples in a jena model. If you pass a ::model the triples will be
  appended to it, otherwise it creates a new model."
  [env triples]
  [(s/keys :req [::jena-model] :opt [::prefixes]) (s/coll-of ::triple-or-res)
   => (s/keys :req [::jena-model])]
  (doseq [t (mapcat expand-resource-statements triples)]
    (tx-statement! env t))
  env)

(>defn update-resource!
  "Update a resource, this will remove previous data in the given keys and add the new
  ones."
  [env {::keys [resource-uri] :as resource}]
  [(s/keys :req [::jena-model]) ::map-resource => ::map-resource]
  (let [resource  (misc/map-keys #(resource-uri-key env %) resource)
        store-res (assoc resource ::resource-uri resource-uri)
        removals  (mapv #(vector ::tx-rem resource-uri % nil) (keys resource))]
    (tx-statements! env (conj removals store-res))
    store-res))

; endregion

; region inference

(>defn ^Model rdfs-model [ontology model]
  [::jena-model ::jena-model => ::jena-model]
  (let [reasoner (.bindSchema (ReasonerRegistry/getRDFSReasoner) ontology)]
    (ModelFactory/createInfModel reasoner model)))

(>defn ^Model owl-model [ontology model]
  [::jena-model ::jena-model => ::jena-model]
  (let [reasoner (.bindSchema (ReasonerRegistry/getOWLReasoner) ontology)]
    (ModelFactory/createInfModel reasoner model)))

; endregion

; region tdb

(>defn ^Dataset tdb-dataset [^String directory]
  [string? => ::jena-dataset]
  (TDBFactory/createDataset directory))

(>defn tdb-read [^Dataset dataset f]
  [::jena-dataset fn? => any?]
  (.begin dataset ReadWrite/READ)
  (try
    (f)
    (catch Throwable e
      (throw e))
    (finally
      (.end dataset))))

(>defn tdb-write! [^Dataset dataset f]
  [::jena-dataset fn? => any?]
  (.begin dataset ReadWrite/WRITE)
  (try
    (let [res (f)]
      (jena-dataset-commit! dataset)
      res)
    (catch Throwable e
      (jena-dataset-abort! dataset)
      (throw e))
    (finally
      (.end dataset))))

; endregion

; region export

(>defn write-stable! [^Model model out]
  [::jena-model ::output-stream => nil?]
  (let [sttl (STTLWriter/registerWriter)]
    (RDFDataMgr/write out model sttl)
    nil))

; endregion
