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
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*

internal class ArangoPersisterTest {

    private val arangoDatabaseMock = mock(ArangoDatabase::class.java)
    private val arangoMock = mock(Arango::class.java)
    private val existingCollection = mock(ArangoCollection::class.java)
    private val nonexistingCollection = mock(ArangoCollection::class.java)
    private val existingGraph = mock(ArangoGraph::class.java)
    private val nonexistingGraph = mock(ArangoGraph::class.java)
    private val name = "test-collection"

    @BeforeEach
    fun init() {
        `when`(existingCollection.exists()).thenReturn(true)
        `when`(nonexistingCollection.exists()).thenReturn(false)
        `when`(existingGraph.exists()).thenReturn(true)
        `when`(nonexistingGraph.exists()).thenReturn(false)
        `when`(arangoMock.getDatabase()).thenReturn(arangoDatabaseMock)
    }

    @Test
    fun prepareExistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(existingCollection)

        val arangoPersistor = ArangoPersister(arangoMock, false)

        arangoPersistor.prepareCollection(name, CollectionType.DOCUMENT)
        verify(existingCollection, never()).drop()
        verify(arangoDatabaseMock, never()).createCollection(eq(name), any())
    }

    @Test
    fun recreateExistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(existingCollection)

        val arangoPersistor = ArangoPersister(arangoMock, true)

        arangoPersistor.prepareCollection(name, CollectionType.DOCUMENT)
        verify(existingCollection, times(1)).drop()
        verify(arangoDatabaseMock, times(1)).createCollection(eq(name), any())
    }


    @Test
    fun prepareNonexistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(nonexistingCollection)

        val arangoPersistor = ArangoPersister(arangoMock, true)

        arangoPersistor.prepareCollection(name, CollectionType.DOCUMENT)
        verify(nonexistingCollection, never()).drop()
        verify(arangoDatabaseMock, times(1)).createCollection(eq(name), any())
    }

    @Test
    fun recreateNonexistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(nonexistingCollection)

        val arangoPersistor = ArangoPersister(arangoMock, false)

        arangoPersistor.prepareCollection(name, CollectionType.DOCUMENT)
        verify(nonexistingCollection, never()).drop()
        verify(arangoDatabaseMock, times(1)).createCollection(eq(name), any())
    }

    @Test
    fun createExistingDatabase() {
        `when`(arangoDatabaseMock.exists()).thenReturn(true)

        val arangoPersistor = ArangoPersister(arangoMock, false)

        arangoPersistor.createDB()
        verify(arangoDatabaseMock, never()).create()
    }

    @Test
    fun createNonexistingDatabase() {
        `when`(arangoDatabaseMock.exists()).thenReturn(false)

        val arangoPersistor = ArangoPersister(arangoMock, false)

        arangoPersistor.createDB()
        verify(arangoDatabaseMock, times(1)).create()
    }

    @Test
    fun prepareExistingGraph() {
        `when`(arangoDatabaseMock.graph(eq(name))).thenReturn(existingGraph)

        val arangoPersistor = ArangoPersister(arangoMock, false)

        arangoPersistor.initGraph(name, EdgeDefinition())
        verify(existingGraph, never()).drop()
        verify(arangoDatabaseMock, never()).createGraph(name, listOf(), null)
    }

    @Test
    fun recreateExistingGraph() {
        `when`(arangoDatabaseMock.graph(eq(name))).thenReturn(existingGraph)

        val arangoPersistor = ArangoPersister(arangoMock, true)

        arangoPersistor.initGraph(name, EdgeDefinition())
        verify(existingGraph, times(1)).drop()
        verify(arangoDatabaseMock, times(1)).createGraph(eq(name), any(), any())
    }

    @Test
    fun prepareNonexistingGraph() {
        `when`(arangoDatabaseMock.graph(eq(name))).thenReturn(nonexistingGraph)

        val arangoPersistor = ArangoPersister(arangoMock, false)

        arangoPersistor.initGraph(name, EdgeDefinition())
        verify(existingGraph, never()).drop()
        verify(arangoDatabaseMock, times(1)).createGraph(eq(name), any(), any())
    }

    @Test
    fun recreateNonexistingGraph() {
        `when`(arangoDatabaseMock.graph(eq(name))).thenReturn(nonexistingGraph)

        val arangoPersistor = ArangoPersister(arangoMock, true)

        arangoPersistor.initGraph(name, EdgeDefinition())
        verify(existingGraph, never()).drop()
        verify(arangoDatabaseMock, times(1)).createGraph(eq(name), any(), any())
    }
}
