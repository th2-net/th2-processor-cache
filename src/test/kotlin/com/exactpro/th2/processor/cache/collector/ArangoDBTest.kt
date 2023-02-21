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
import com.arangodb.entity.CollectionType
import com.exactpro.th2.cache.common.Arango
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.*

internal class ArangoDBTest {

    private val arangoDatabaseMock = mock(ArangoDatabase::class.java)
    private val arangoMock = mock(Arango::class.java)
    private val existingCollection = mock(ArangoCollection::class.java)
    private val nonexistingCollection = mock(ArangoCollection::class.java)
    private val name = "test-collection"

    @BeforeEach
    fun init() {
        `when`(existingCollection.exists()).thenReturn(true)
        `when`(nonexistingCollection.exists()).thenReturn(false)
        `when`(arangoMock.getDatabase()).thenReturn(arangoDatabaseMock)
    }

    @Test
    fun prepareExistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(existingCollection)

        val arangoDb = ArangoDB(arangoMock, true)

        arangoDb.prepareCollection(name, CollectionType.DOCUMENT, false)
        verify(existingCollection, never()).drop()
        verify(arangoDatabaseMock, never()).createCollection(eq(name), any())
    }

    @Test
    fun recreateExistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(existingCollection)

        val arangoDb = ArangoDB(arangoMock, true)

        arangoDb.prepareCollection(name, CollectionType.DOCUMENT, true)
        verify(existingCollection, times(1)).drop()
        verify(arangoDatabaseMock, times(1)).createCollection(eq(name), any())
    }


    @Test
    fun prepareNonexistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(nonexistingCollection)

        val arangoDb = ArangoDB(arangoMock, true)

        arangoDb.prepareCollection(name, CollectionType.DOCUMENT, true)
        verify(nonexistingCollection, never()).drop()
        verify(arangoDatabaseMock, times(1)).createCollection(eq(name), any())
    }

    @Test
    fun recreateNonexistingCollection() {
        `when`(arangoDatabaseMock.collection(eq(name))).thenReturn(nonexistingCollection)

        val arangoDb = ArangoDB(arangoMock, true)

        arangoDb.prepareCollection(name, CollectionType.DOCUMENT, false)
        verify(nonexistingCollection, never()).drop()
        verify(arangoDatabaseMock, times(1)).createCollection(eq(name), any())
    }
}