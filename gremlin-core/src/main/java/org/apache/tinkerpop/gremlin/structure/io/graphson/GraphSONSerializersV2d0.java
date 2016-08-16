/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Comparators;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdKeySerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil.writeEndArray;
import static org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil.writeEndObject;
import static org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil.writeStartArray;
import static org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil.writeStartObject;

/**
 * GraphSON serializers for graph-based objects such as vertices, edges, properties, and paths. These serializers
 * present a generalized way to serialize the implementations of core interfaces.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONSerializersV2d0 {

    private GraphSONSerializersV2d0() {
    }

    final static class VertexPropertyJacksonSerializer extends StdSerializer<VertexProperty> {

        private final boolean normalize;

        public VertexPropertyJacksonSerializer(final boolean normalize) {
            super(VertexProperty.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final VertexProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            serializerVertexProperty(property, jsonGenerator, serializerProvider, null, normalize, true);
        }

        @Override
        public void serializeWithType(final VertexProperty property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            serializerVertexProperty(property, jsonGenerator, serializerProvider, typeSerializer, normalize, true);
        }
    }

    final static class PropertyJacksonSerializer extends StdSerializer<Property> {

        public PropertyJacksonSerializer() {
            super(Property.class);
        }

        @Override
        public void serialize(final Property property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(property, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final Property property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(property, jsonGenerator, serializerProvider, typeSerializer);
        }

        private static void ser(final Property property, final JsonGenerator jsonGenerator,
                                final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            if (typeSerializer != null) {
                typeSerializer.writeTypePrefixForScalar(property, jsonGenerator);
            }
            writeStartObject(property, jsonGenerator, typeSerializer);

            serializerProvider.defaultSerializeField(GraphSONTokens.KEY, property.key(), jsonGenerator);
            serializerProvider.defaultSerializeField(GraphSONTokens.VALUE, property.value(), jsonGenerator);

            writeEndObject(property, jsonGenerator, typeSerializer);
            if (typeSerializer != null) {
                typeSerializer.writeTypeSuffixForScalar(property, jsonGenerator);
            }
        }
    }

    final static class EdgeJacksonSerializer extends StdSerializer<Edge> {

        private final boolean normalize;

        public EdgeJacksonSerializer(final boolean normalize) {
            super(Edge.class);
            this.normalize = normalize;
        }


        @Override
        public void serialize(final Edge edge, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(edge, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final Edge edge, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(edge, jsonGenerator, serializerProvider, typeSerializer);
        }

        private void ser(final Edge edge, final JsonGenerator jsonGenerator,
                         final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            if (typeSerializer != null) {
                typeSerializer.writeTypePrefixForScalar(edge, jsonGenerator);
            }

            writeStartObject(edge, jsonGenerator, typeSerializer);

            GraphSONUtil.writeWithType(GraphSONTokens.ID, edge.id(), jsonGenerator, serializerProvider, typeSerializer);
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, edge.label());
//            jsonGenerator.writeStringField(GraphSONTokens.TYPE, GraphSONTokens.EDGE);
            jsonGenerator.writeStringField(GraphSONTokens.IN_LABEL, edge.inVertex().label());
            jsonGenerator.writeStringField(GraphSONTokens.OUT_LABEL, edge.outVertex().label());
            GraphSONUtil.writeWithType(GraphSONTokens.IN, edge.inVertex().id(), jsonGenerator, serializerProvider, typeSerializer);
            GraphSONUtil.writeWithType(GraphSONTokens.OUT, edge.outVertex().id(), jsonGenerator, serializerProvider, typeSerializer);
            writeProperties(edge, jsonGenerator, serializerProvider, typeSerializer);

            writeEndObject(edge, jsonGenerator, typeSerializer);

            if (typeSerializer != null) {
                typeSerializer.writeTypeSuffixForScalar(edge, jsonGenerator);
            }
        }

        private void writeProperties(final Edge edge, final JsonGenerator jsonGenerator,
                                     final SerializerProvider serializerProvider,
                                     final TypeSerializer typeSerializer) throws IOException {
            final Iterator<Property<Object>> elementProperties = normalize ?
                    IteratorUtils.list(edge.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : edge.properties();
            if (elementProperties.hasNext()) {
                jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);
                writeStartObject(edge, jsonGenerator, typeSerializer);

                while (elementProperties.hasNext()) {
                    final Property<Object> elementProperty = elementProperties.next();
                    jsonGenerator.writeObjectField(elementProperty.key(), elementProperty);
                }

                writeEndObject(edge, jsonGenerator, typeSerializer);
            }
        }

    }

    final static class VertexJacksonSerializer extends StdSerializer<Vertex> {

        private final boolean normalize;

        public VertexJacksonSerializer(final boolean normalize) {
            super(Vertex.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final Vertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(vertex, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final Vertex vertex, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(vertex, jsonGenerator, serializerProvider, typeSerializer);
        }

        private void ser(final Vertex vertex, final JsonGenerator jsonGenerator,
                         final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException {
            if (typeSerializer != null) {
                typeSerializer.writeTypePrefixForScalar(vertex, jsonGenerator);
            }

            writeStartObject(vertex, jsonGenerator, typeSerializer);

            GraphSONUtil.writeWithType(GraphSONTokens.ID, vertex.id(), jsonGenerator, serializerProvider, typeSerializer);
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, vertex.label());
//            jsonGenerator.writeStringField(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);
            writeProperties(vertex, jsonGenerator, serializerProvider, typeSerializer);

            writeEndObject(vertex, jsonGenerator, typeSerializer);

            if (typeSerializer != null) {
                typeSerializer.writeTypeSuffixForScalar(vertex, jsonGenerator);
            }
        }

        private void writeProperties(final Vertex vertex, final JsonGenerator jsonGenerator,
                                     final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);
            writeStartObject(vertex, jsonGenerator, typeSerializer);

            final List<String> keys = normalize ?
                    IteratorUtils.list(vertex.keys().iterator(), Comparator.naturalOrder()) : new ArrayList<>(vertex.keys());
            for (String key : keys) {
                final Iterator<VertexProperty<Object>> vertexProperties = normalize ?
                        IteratorUtils.list(vertex.properties(key), Comparators.PROPERTY_COMPARATOR).iterator() : vertex.properties(key);
                if (vertexProperties.hasNext()) {
                    jsonGenerator.writeFieldName(key);
                    writeStartArray(vertex, jsonGenerator, typeSerializer);

                    while (vertexProperties.hasNext()) {
                        serializerVertexProperty(vertexProperties.next(), jsonGenerator, serializerProvider, typeSerializer, normalize, false);
                    }

                    writeEndArray(vertex, jsonGenerator, typeSerializer);
                }
            }

            writeEndObject(vertex, jsonGenerator, typeSerializer);
        }

    }

    final static class PathJacksonSerializer extends StdSerializer<Path> {

        public PathJacksonSerializer() {
            super(Path.class);
        }

        @Override
        public void serialize(final Path path, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            ser(path, jsonGenerator, null);
        }

        @Override
        public void serializeWithType(final Path path, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException, JsonProcessingException {
            ser(path, jsonGenerator, typeSerializer);
        }

        private static void ser(final Path path, final JsonGenerator jsonGenerator, final TypeSerializer typeSerializer)
                throws IOException {
            if (typeSerializer != null) {
                typeSerializer.writeTypePrefixForScalar(path, jsonGenerator);
            }
            writeStartObject(path, jsonGenerator, typeSerializer);

            jsonGenerator.writeObjectField(GraphSONTokens.LABELS, path.labels());
            jsonGenerator.writeObjectField(GraphSONTokens.OBJECTS, path.objects());

            writeEndObject(path, jsonGenerator, typeSerializer);
            if (typeSerializer != null) {
                typeSerializer.writeTypeSuffixForScalar(path, jsonGenerator);
            }
        }

    }

    final static class TreeJacksonSerializer extends StdSerializer<Tree> {

        public TreeJacksonSerializer() {
            super(Tree.class);
        }

        @Override
        public void serialize(final Tree tree, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            ser(tree, jsonGenerator, null);
        }

        @Override
        public void serializeWithType(final Tree tree, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException, JsonProcessingException {
            ser(tree, jsonGenerator, typeSerializer);
        }

        private static void ser(final Tree tree, final JsonGenerator jsonGenerator, final TypeSerializer typeSerializer)
                throws IOException {
            writeStartObject(tree, jsonGenerator, typeSerializer);

            Set<Map.Entry<Element, Tree>> set = tree.entrySet();
            for (Map.Entry<Element, Tree> entry : set) {
                jsonGenerator.writeFieldName(entry.getKey().id().toString());
                writeStartObject(entry, jsonGenerator, typeSerializer);
                jsonGenerator.writeObjectField(GraphSONTokens.KEY, entry.getKey());
                jsonGenerator.writeObjectField(GraphSONTokens.VALUE, entry.getValue());
                writeEndObject(entry, jsonGenerator, typeSerializer);
            }

            writeEndObject(tree, jsonGenerator, typeSerializer);
        }
    }

    /**
     * Maps in the JVM can have {@link Object} as a key, but in JSON they must be a {@link String}.
     */
    final static class GraphSONKeySerializer extends StdKeySerializer {

        @Override
        public void serialize(final Object o, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {
            ser(o, jsonGenerator, serializerProvider);
        }

        @Override
        public void serializeWithType(final Object o, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(o, jsonGenerator, serializerProvider);
        }

        private void ser(final Object o, final JsonGenerator jsonGenerator,
                         final SerializerProvider serializerProvider) throws IOException {
            if (Element.class.isAssignableFrom(o.getClass()))
                jsonGenerator.writeFieldName((((Element) o).id()).toString());
            else
                super.serialize(o, jsonGenerator, serializerProvider);
        }
    }

    final static class TraversalExplanationJacksonSerializer extends StdSerializer<TraversalExplanation> {
        public TraversalExplanationJacksonSerializer() {
            super(TraversalExplanation.class);
        }

        @Override
        public void serialize(final TraversalExplanation traversalExplanation, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            ser(traversalExplanation, jsonGenerator);
        }

        @Override
        public void serializeWithType(final TraversalExplanation value, final JsonGenerator gen,
                                      final SerializerProvider serializers, final TypeSerializer typeSer) throws IOException {
            ser(value, gen);
        }

        public void ser(final TraversalExplanation te, final JsonGenerator jsonGenerator) throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ORIGINAL, getStepsAsList(te.getOriginalTraversal()));

            final List<Pair<TraversalStrategy, Traversal.Admin<?, ?>>> strategyTraversals = te.getStrategyTraversals();

            final List<Map<String, Object>> intermediates = new ArrayList<>();
            for (final Pair<TraversalStrategy, Traversal.Admin<?, ?>> pair : strategyTraversals) {
                final Map<String, Object> intermediate = new HashMap<>();
                intermediate.put(GraphSONTokens.STRATEGY, pair.getValue0().toString());
                intermediate.put(GraphSONTokens.CATEGORY, pair.getValue0().getTraversalCategory().getSimpleName());
                intermediate.put(GraphSONTokens.TRAVERSAL, getStepsAsList(pair.getValue1()));
                intermediates.add(intermediate);
            }
            m.put(GraphSONTokens.INTERMEDIATE, intermediates);

            if (strategyTraversals.isEmpty())
                m.put(GraphSONTokens.FINAL, getStepsAsList(te.getOriginalTraversal()));
            else
                m.put(GraphSONTokens.FINAL, getStepsAsList(strategyTraversals.get(strategyTraversals.size() - 1).getValue1()));

            jsonGenerator.writeObject(m);
        }

        private List<String> getStepsAsList(final Traversal.Admin<?, ?> t) {
            final List<String> steps = new ArrayList<>();
            t.getSteps().iterator().forEachRemaining(s -> steps.add(s.toString()));
            return steps;
        }
    }

    final static class IntegerGraphSONSerializer extends StdSerializer<Integer> {
        public IntegerGraphSONSerializer() {
            super(Integer.class);
        }

        @Override
        public void serialize(Integer integer, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeNumber(((Integer) integer).intValue());
        }

        @Override
        public void serializeWithType(Integer integer, JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer) throws IOException {
            typeSerializer.writeTypePrefixForScalar(integer, jsonGenerator);
            serialize(integer, jsonGenerator, serializerProvider);
            typeSerializer.writeTypeSuffixForScalar(integer, jsonGenerator);
        }
    }

    final static class DoubleGraphSONSerializer extends StdSerializer<Double> {
        public DoubleGraphSONSerializer() {
            super(Double.class);
        }

        @Override
        public void serialize(Double doubleValue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeNumber(doubleValue);
        }

        @Override
        public void serializeWithType(Double doubleValue, JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer) throws IOException {
            typeSerializer.writeTypePrefixForScalar(doubleValue, jsonGenerator);
            serialize(doubleValue, jsonGenerator, serializerProvider);
            typeSerializer.writeTypeSuffixForScalar(doubleValue, jsonGenerator);
        }
    }

    final static class TraversalMetricsJacksonSerializer extends StdSerializer<TraversalMetrics> {
        public TraversalMetricsJacksonSerializer() {
            super(TraversalMetrics.class);
        }

        @Override
        public void serialize(final TraversalMetrics property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            serializeInternal(property, jsonGenerator);
        }

        @Override
        public void serializeWithType(final TraversalMetrics property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            serializeInternal(property, jsonGenerator);
        }

        private static void serializeInternal(final TraversalMetrics traversalMetrics, final JsonGenerator jsonGenerator) throws IOException {
            // creation of the map enables all the fields to be properly written with their type if required
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.DURATION, traversalMetrics.getDuration(TimeUnit.NANOSECONDS) / 1000000d);
            final List<Map<String, Object>> metrics = new ArrayList<>();
            traversalMetrics.getMetrics().forEach(it -> metrics.add(metricsToMap(it)));
            m.put(GraphSONTokens.METRICS, metrics);

            jsonGenerator.writeObject(m);
        }

        private static Map<String, Object> metricsToMap(final Metrics metrics) {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, metrics.getId());
            m.put(GraphSONTokens.NAME, metrics.getName());
            m.put(GraphSONTokens.COUNTS, metrics.getCounts());
            m.put(GraphSONTokens.DURATION, metrics.getDuration(TimeUnit.NANOSECONDS) / 1000000d);

            if (!metrics.getAnnotations().isEmpty()) {
                m.put(GraphSONTokens.ANNOTATIONS, metrics.getAnnotations());
            }

            if (!metrics.getNested().isEmpty()) {
                final List<Map<String, Object>> nested = new ArrayList<>();
                metrics.getNested().forEach(it -> nested.add(metricsToMap(it)));
                m.put(GraphSONTokens.METRICS, nested);
            }
            return m;
        }
    }

    private static void serializerVertexProperty(final VertexProperty property, final JsonGenerator jsonGenerator,
                                                 final SerializerProvider serializerProvider,
                                                 final TypeSerializer typeSerializer, final boolean normalize,
                                                 final boolean includeLabel) throws IOException {
        writeStartObject(property, jsonGenerator, typeSerializer);

        GraphSONUtil.writeWithType(GraphSONTokens.ID, property.id(), jsonGenerator, serializerProvider, typeSerializer);
        GraphSONUtil.writeWithType(GraphSONTokens.VALUE, property.value(), jsonGenerator, serializerProvider, typeSerializer);
        if (includeLabel)
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, property.label());
        tryWriteMetaProperties(property, jsonGenerator, serializerProvider, typeSerializer, normalize);

        writeEndObject(property, jsonGenerator, typeSerializer);
    }

    private static void tryWriteMetaProperties(final VertexProperty property, final JsonGenerator jsonGenerator,
                                               final SerializerProvider serializerProvider,
                                               final TypeSerializer typeSerializer, final boolean normalize) throws IOException {
        // when "detached" you can't check features of the graph it detached from so it has to be
        // treated differently from a regular VertexProperty implementation.
        if (property instanceof DetachedVertexProperty) {
            // only write meta properties key if they exist
            if (property.properties().hasNext()) {
                writeMetaProperties(property, jsonGenerator, serializerProvider, typeSerializer, normalize);
            }
        } else {
            // still attached - so we can check the features to see if it's worth even trying to write the
            // meta properties key
            if (property.graph().features().vertex().supportsMetaProperties() && property.properties().hasNext()) {
                writeMetaProperties(property, jsonGenerator, serializerProvider, typeSerializer, normalize);
            }
        }
    }

    private static void writeMetaProperties(final VertexProperty property, final JsonGenerator jsonGenerator,
                                            final SerializerProvider serializerProvider,
                                            final TypeSerializer typeSerializer, final boolean normalize) throws IOException {
        jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);
        writeStartObject(property, jsonGenerator, typeSerializer);

        final Iterator<Property<Object>> metaProperties = normalize ?
                IteratorUtils.list((Iterator<Property<Object>>) property.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : property.properties();
        while (metaProperties.hasNext()) {
            final Property<Object> metaProperty = metaProperties.next();
            GraphSONUtil.writeWithType(metaProperty.key(), metaProperty.value(), jsonGenerator, serializerProvider, typeSerializer);
        }

        writeEndObject(property, jsonGenerator, typeSerializer);
    }


    static class VertexJacksonDeserializer extends StdDeserializer<Vertex> {

        protected VertexJacksonDeserializer() {
            super(Vertex.class);
        }

        @Override
        public Vertex deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

            jsonParser.nextToken();
            // This will automatically parse VertexProperties and other typed stuff automatically.
            Map<String, Object> vertexData = deserializationContext.readValue(jsonParser, Map.class);

            final DetachedVertex detached = new DetachedVertex(
                    vertexData.get(GraphSONTokens.ID),
                    vertexData.get(GraphSONTokens.LABEL).toString(),
                    (Map<String, Object>) vertexData.get(GraphSONTokens.PROPERTIES)
            );
            return detached;
        }
    }

    static class EdgeJacksonDeserializer extends StdDeserializer<Edge> {

        protected EdgeJacksonDeserializer() {
            super(Edge.class);
        }

        @Override
        public Edge deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

            jsonParser.nextToken();
            // This will automatically parse Properties and other typed stuff automatically.
            Map<String, Object> edgeData = deserializationContext.readValue(jsonParser, Map.class);

            final DetachedEdge detached = new DetachedEdge(
                    edgeData.get(GraphSONTokens.ID),
                    edgeData.get(GraphSONTokens.LABEL).toString(),
                    (Map)edgeData.get(GraphSONTokens.PROPERTIES),
                    Pair.with(edgeData.get(GraphSONTokens.OUT), edgeData.get(GraphSONTokens.OUT_LABEL).toString()),
                    Pair.with(edgeData.get(GraphSONTokens.IN), edgeData.get(GraphSONTokens.IN_LABEL).toString())
            );
            return detached;
        }
    }

    static class PropertyJacksonDeserializer extends StdDeserializer<Property> {

        protected PropertyJacksonDeserializer() {
            super(Property.class);
        }

        @Override
        public Property deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

            jsonParser.nextToken();
            Map propData = deserializationContext.readValue(jsonParser, Map.class);

            final DetachedProperty detached = new DetachedProperty((String)propData.get(GraphSONTokens.KEY), propData.get(GraphSONTokens.VALUE));
            return detached;
        }
    }

    static class PathJacksonDeserializer extends StdDeserializer<Path> {

        protected PathJacksonDeserializer() {
            super(Path.class);
        }

        @Override
        public Path deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

            jsonParser.nextToken();
            Map<String, Object> pathData = deserializationContext.readValue(jsonParser, Map.class);
            Path p = MutablePath.make();

            List labels = (List)pathData.get(GraphSONTokens.LABELS);
            List objects = (List)pathData.get(GraphSONTokens.OBJECTS);

            for (int i = 0; i < objects.size(); i++) {
                p.extend(objects.get(i), new HashSet((List)labels.get(i)));
            }

            return p;
        }
    }
}



