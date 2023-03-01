/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.processor.cache.collector

import com.arangodb.ArangoCollection
import com.arangodb.ArangoDatabase
import com.arangodb.ArangoGraph
import com.arangodb.entity.CollectionType
import com.arangodb.entity.EdgeDefinition
import com.exactpro.th2.cache.common.Arango
import com.exactpro.th2.cache.common.ArangoCredentials
import com.exactpro.th2.cache.common.event.Event
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.processor.cache.collector.event.toCacheEvent
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.grpc.Grpc
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*
import java.time.Instant

internal class ArangoDBTest {

    private val arangoDatabaseMock = mock(ArangoDatabase::class.java)
    private val arangoMock = mock(Arango::class.java)
    private val existingCollection = mock(ArangoCollection::class.java)
    private val nonexistingCollection = mock(ArangoCollection::class.java)
    private val existingGraph = mock(ArangoGraph::class.java)
    private val nonexistingGraph = mock(ArangoGraph::class.java)
    private val name = "test-collection"
    private lateinit var processor: Processor
    private val eventBatcherMock = mock(EventBatcher::class.java)
    private val processorEventIdMock = mock(EventID::class.java)
    private val arangoCredentialsMock = mock(ArangoCredentials::class.java)
    private val settings = Settings(arangoCredentialsMock)
    private val arangoDb = mock(ArangoDB::class.java)

    @BeforeEach
    fun init() {
        processor = Processor(eventBatcherMock, processorEventIdMock, settings, arangoDb)

        `when`(existingCollection.exists()).thenReturn(true)
        `when`(nonexistingCollection.exists()).thenReturn(false)
        `when`(existingGraph.exists()).thenReturn(true)
        `when`(nonexistingGraph.exists()).thenReturn(false)
        `when`(arangoMock.getDatabase()).thenReturn(arangoDatabaseMock)
    }

    @Test
    fun prepareExistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(existingCollection)

        val arangoDb = ArangoDB(arangoMock, false)

        arangoDb.prepareCollection(name, CollectionType.DOCUMENT)
        verify(existingCollection, never()).drop()
        verify(arangoDatabaseMock, never()).createCollection(eq(name), any())
    }

    @Test
    fun recreateExistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(existingCollection)

        val arangoDb = ArangoDB(arangoMock, true)

        arangoDb.prepareCollection(name, CollectionType.DOCUMENT)
        verify(existingCollection, times(1)).drop()
        verify(arangoDatabaseMock, times(1)).createCollection(eq(name), any())
    }


    @Test
    fun prepareNonexistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(nonexistingCollection)

        val arangoDb = ArangoDB(arangoMock, true)

        arangoDb.prepareCollection(name, CollectionType.DOCUMENT)
        verify(nonexistingCollection, never()).drop()
        verify(arangoDatabaseMock, times(1)).createCollection(eq(name), any())
    }

    @Test
    fun recreateNonexistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(nonexistingCollection)

        val arangoDb = ArangoDB(arangoMock, false)

        arangoDb.prepareCollection(name, CollectionType.DOCUMENT)
        verify(nonexistingCollection, never()).drop()
        verify(arangoDatabaseMock, times(1)).createCollection(eq(name), any())
    }

    @Test
    fun createExistingDatabase() {
        `when`(arangoDatabaseMock.exists()).thenReturn(true)

        val arangoDb = ArangoDB(arangoMock, false)

        arangoDb.createDB()
        verify(arangoDatabaseMock, never()).create()
    }

    @Test
    fun createNonexistingDatabase() {
        `when`(arangoDatabaseMock.exists()).thenReturn(false)

        val arangoDb = ArangoDB(arangoMock, false)

        arangoDb.createDB()
        verify(arangoDatabaseMock, times(1)).create()
    }

    @Test
    fun prepareExistingGraph() {
        `when`(arangoDatabaseMock.graph(eq(name))).thenReturn(existingGraph)

        val arangoDb = ArangoDB(arangoMock, false)

        arangoDb.initGraph(name, EdgeDefinition())
        verify(existingGraph, never()).drop()
        verify(arangoDatabaseMock, never()).createGraph(name, listOf(), null)
    }

    @Test
    fun recreateExistingGraph() {
        `when`(arangoDatabaseMock.graph(eq(name))).thenReturn(existingGraph)

        val arangoDb = ArangoDB(arangoMock, true)

        arangoDb.initGraph(name, EdgeDefinition())
        verify(existingGraph, times(1)).drop()
        verify(arangoDatabaseMock, times(1)).createGraph(eq(name), any(), any())
    }

    @Test
    fun prepareNonexistingGraph() {
        `when`(arangoDatabaseMock.graph(eq(name))).thenReturn(nonexistingGraph)

        val arangoDb = ArangoDB(arangoMock, false)

        arangoDb.initGraph(name, EdgeDefinition())
        verify(existingGraph, never()).drop()
        verify(arangoDatabaseMock, times(1)).createGraph(eq(name), any(), any())
    }

    @Test
    fun recreateNonexistingGraph() {
        `when`(arangoDatabaseMock.graph(eq(name))).thenReturn(nonexistingGraph)

        val arangoDb = ArangoDB(arangoMock, true)

        arangoDb.initGraph(name, EdgeDefinition())
        verify(existingGraph, never()).drop()
        verify(arangoDatabaseMock, times(1)).createGraph(eq(name), any(), any())
    }

    @Test
    fun callsInsertEvents() {
        val list = mutableListOf<GrpcEvent>()
        for (i in 1..100) {
            val grpcEvent = GrpcEvent.newBuilder()
                .setId(EVENT_ID)
                .setEndTimestamp(Timestamp.newBuilder().build())
                .setStatus(EventStatus.SUCCESS)
                .setName(name)
                .setType("type")
                .setBody(ByteString.EMPTY)
                .build()
            list.add(grpcEvent)
        }

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        verify(arangoDb, times(1)).insertEvents(list.map { it.toCacheEvent() })
    }

    companion object {
        private const val BOOK_NAME = "known-book"
        private const val SCOPE_NAME = "known-scope"

        private val FROM = Instant.now()
        private val TO = FROM.plusSeconds(1_000)
        private val PROCESSOR_EVENT_ID = EventID.newBuilder().apply {
            bookName = BOOK_NAME
            scope = SCOPE_NAME
        }.build()
        private val EVENT_ID = EventID.newBuilder().setId("id").setBookName(BOOK_NAME).setScope(SCOPE_NAME).setStartTimestamp(Timestamp.newBuilder().build()).build()
        private val INTERVAL_EVENT_ID = PROCESSOR_EVENT_ID.toBuilder().build()
    }
}
