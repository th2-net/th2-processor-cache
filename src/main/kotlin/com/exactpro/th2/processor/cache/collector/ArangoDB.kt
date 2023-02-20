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
import com.arangodb.entity.BaseEdgeDocument
import com.arangodb.entity.CollectionType
import com.arangodb.entity.EdgeDefinition
import com.arangodb.model.CollectionCreateOptions
import com.exactpro.th2.cache.common.Arango
import com.exactpro.th2.cache.common.message.ParsedMessage
import com.exactpro.th2.cache.common.message.RawMessage
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.processor.cache.collector.message.getParentMessageId
import com.exactpro.th2.processor.cache.collector.message.hasParentMessage
import mu.KotlinLogging

class ArangoDB(
    settings: Settings
) {
    private val recreateCollections: Boolean = settings.recreateCollections
    private val arango: Arango = Arango(settings.arangoCredentials)

    internal val database: ArangoDatabase = arango.getDatabase()

    private lateinit var rawMessageCollection: ArangoCollection
    private lateinit var parsedMessageCollection: ArangoCollection
    private lateinit var eventCollection: ArangoCollection
    private lateinit var eventRelationshipCollection: ArangoCollection
    private lateinit var parsedMessageRelationshipCollection: ArangoCollection

    private fun getEventKey(eventId : String): String = Arango.EVENT_COLLECTION + "/" + eventId

    private fun getMessageKey(messageId: String): String = Arango.PARSED_MESSAGE_COLLECTION + "/" + messageId

    internal fun initCollections(reportEventId: EventID) {
        initCollections(reportEventId, mapOf(
            Arango.EVENT_COLLECTION to CollectionType.DOCUMENT,
            Arango.RAW_MESSAGE_COLLECTION to CollectionType.DOCUMENT,
            Arango.PARSED_MESSAGE_COLLECTION to CollectionType.DOCUMENT,
            Arango.EVENT_EDGES to CollectionType.EDGES,
            Arango.MESSAGE_EDGES to CollectionType.EDGES
            )
        )

        eventRelationshipCollection = database.collection(Arango.EVENT_EDGES)
        parsedMessageRelationshipCollection = database.collection(Arango.MESSAGE_EDGES)
        eventCollection = database.collection(Arango.EVENT_COLLECTION)
        rawMessageCollection = database.collection(Arango.RAW_MESSAGE_COLLECTION)
        parsedMessageCollection = database.collection(Arango.PARSED_MESSAGE_COLLECTION)
    }

    internal fun initGraphs() {
        val eventGraphEdgeDefinition: EdgeDefinition = EdgeDefinition()
            .collection(Arango.EVENT_EDGES)
            .from(Arango.EVENT_COLLECTION)
            .to(Arango.EVENT_COLLECTION)

        val messageGraphEdgeDefinition: EdgeDefinition = EdgeDefinition()
            .collection(Arango.MESSAGE_EDGES)
            .from(Arango.PARSED_MESSAGE_COLLECTION)
            .to(Arango.PARSED_MESSAGE_COLLECTION)

        initGraph(Arango.EVENT_GRAPH, eventGraphEdgeDefinition)
        initGraph(Arango.MESSAGE_GRAPH, messageGraphEdgeDefinition)
    }

    internal fun createDB() {
        if (!database.exists()) {
            database.create()
        }
    }

    private fun initGraph(name: String, edgeDefinition: EdgeDefinition) {
        val graph = database.graph(name)
        var exists = graph.exists()
        if (exists && recreateCollections) {
            K_LOGGER.info { "Dropping graph \"${name}\"" }
            graph.drop()
            exists = false
        }
        if (!exists) {
            K_LOGGER.info { "Creating graph \"${name}\"" }
            database.createGraph(name, mutableListOf(edgeDefinition), null)
        }
    }

    private fun initCollections(reportEventId: EventID, collections: Map<String, CollectionType>) {
        collections.forEach {
            val name = it.key
            val type = it.value
            kotlin.runCatching {
                val collection = database.collection(name)
                var exists = collection.exists()
                if (exists && recreateCollections) {
                    K_LOGGER.info { "Dropping collection \"${name}\"" }
                    database.collection(name).drop()
                    exists = false
                }
                if (!exists) {
                    K_LOGGER.info { "Creating collection \"${name}\"" }
                    database.createCollection(name, CollectionCreateOptions().type(type))
                }
            }.onFailure { e ->
                throw e
            }.onSuccess {

            }.getOrThrow()
        }
    }

    internal fun insertParsedMessages(messages: List<ParsedMessage>) {
        try {
            parsedMessageCollection.insertDocuments(messages)
            parsedMessageRelationshipCollection.insertDocuments(messages.filter { el -> el.hasParentMessage() }
                .map { el ->
                    BaseEdgeDocument().apply {
                        from = getMessageKey(el.getParentMessageId())
                        to = getMessageKey(el.id)
                    }
                }
            )
        } catch (e: Exception) {
            K_LOGGER.error { "${e.message}" }
        }
    }

    internal fun insertRawMessages(messages: List<RawMessage>) {
        try {
            rawMessageCollection.insertDocuments(messages)
        } catch (e: Exception) {
            K_LOGGER.error { "${e.message}" }
        }
    }

    internal fun insertEvents(events: List<com.exactpro.th2.cache.common.event.Event>) {
        try {
            eventCollection.insertDocuments(events)
            eventRelationshipCollection.insertDocuments(events.filter { el -> el.parentEventId != null }
                .map { el ->
                    BaseEdgeDocument().apply {
                        from = getEventKey(el.parentEventId!!)
                        to = getEventKey(el.eventId)
                    }
                }
            )
        } catch (e: Exception) {
            K_LOGGER.error { "${e.message}" }
        }
    }

    companion object {
        val K_LOGGER = KotlinLogging.logger {}
    }
}
