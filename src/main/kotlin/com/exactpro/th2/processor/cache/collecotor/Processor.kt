/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.processor.cache.collecotor

import com.arangodb.ArangoDB
import com.arangodb.ArangoDatabase
import com.arangodb.ArangoEdgeCollection
import com.arangodb.ArangoVertexCollection
import com.arangodb.DbName
import com.arangodb.entity.BaseEdgeDocument
import com.arangodb.entity.CollectionType
import com.arangodb.entity.EdgeDefinition
import com.arangodb.model.CollectionCreateOptions
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.cache.collecotor.event.CacheEvent.Companion.cacheId
import com.exactpro.th2.processor.cache.collecotor.event.CacheEvent.Companion.toCacheEvent
import com.exactpro.th2.processor.cache.collecotor.message.CacheMessage.Companion.cacheId
import com.exactpro.th2.processor.cache.collecotor.message.CacheMessage.Companion.toCacheMessage
import com.exactpro.th2.processor.utility.log
import mu.KotlinLogging

typealias GrpcEvent = com.exactpro.th2.common.grpc.Event
typealias GrpcMessage = Message
typealias EventBuilder = Event

class Processor(
    private val eventBatcher: EventBatcher,
    processorEventId: EventID,
    settings: Settings,
) : IProcessor {

    private val eventHierarchyGraph = settings.eventHierarchyGraph
    private val edgeCollection = settings.edgeCollection
    private val messageCollection = settings.messageCollection
    private val eventCollection = settings.eventCollection

    private val arangoDB: ArangoDB = settings.createArangoDB()
    private val arangoDatabase: ArangoDatabase = arangoDB.db(DbName.of(settings.arangoDbName))
    private val messageVertexCollection: ArangoVertexCollection
    private val eventVertexCollection: ArangoVertexCollection
    private val edgeVertexCollection: ArangoEdgeCollection

    init {
        arangoDatabase.createDB(processorEventId)

        eventCollection?.let {
            arangoDatabase.recreateCollection(processorEventId, eventCollection, CollectionType.DOCUMENT)
        }
        messageCollection?.let {
            arangoDatabase.recreateCollection(processorEventId, messageCollection, CollectionType.DOCUMENT)
        }
        edgeCollection?.let {
            arangoDatabase.recreateCollection(processorEventId, edgeCollection, CollectionType.EDGES)
        }

        val edgeDefinition: EdgeDefinition = EdgeDefinition()
            .collection(edgeCollection)
            .from(eventCollection)
            .to(eventCollection)

        arangoDatabase.createGraph(eventHierarchyGraph, mutableListOf(edgeDefinition), null)
        with(arangoDatabase.graph(eventHierarchyGraph)) {
            edgeVertexCollection = edgeCollection(edgeCollection).apply {
                insertEdge(edgeDefinition)
            }
            eventVertexCollection = vertexCollection(eventCollection)
            messageVertexCollection = vertexCollection(messageCollection)
        }
    }

    override fun handle(intervalEventId: EventID, event: GrpcEvent) {
        storeDocument(event)
        if (event.hasParentId()) {
            storeEdge(event.id, event.parentId)
        }
        event.attachedMessageIdsList.forEach { messageId ->
            // FIXME: maybe store as a batch
            storeEdge(event.id, messageId)
        }
    }

    override fun handle(intervalEventId: EventID, message: Message) {
        storeDocument(message)
    }

    private fun storeDocument(event: GrpcEvent) {
        eventVertexCollection.insertVertex(event.toCacheEvent())
    }

    private fun storeDocument(message: GrpcMessage) {
        messageVertexCollection.insertVertex(message.toCacheMessage())
    }

    private fun storeEdge(id: EventID, parentId: EventID) {
        edgeVertexCollection.insertEdge(BaseEdgeDocument().apply {
            from = parentId.cacheId
            to = id.cacheId
        })
    }

    private fun storeEdge(id: EventID, messageId: MessageID) {
        // FIXME: I'm not sure about using the same edge
        edgeVertexCollection.insertEdge(BaseEdgeDocument().apply {
            from = id.cacheId
            to = messageId.cacheId
        })
    }

    private fun ArangoDatabase.createDB(
        reportEventId: EventID,
    ) {
        runCatching {
            if (!exists()) {
                create()
                eventBatcher.onEvent(
                    EventBuilder.start()
                        .name("Created ${dbName()} database")
                        .type(EVENT_TYPE_INIT_DATABASE)
                        .toProto(reportEventId)
                        .log(K_LOGGER)
                )
            }
        }.onFailure { e ->
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to create ${dbName()} database")
                    .type(EVENT_TYPE_INIT_DATABASE)
                    .status(Event.Status.FAILED)
                    .exception(e, true)
                    .toProto(reportEventId)
                    .log(K_LOGGER)
            )
            throw e
        }.getOrThrow()
    }

    private fun ArangoDatabase.recreateCollection(
        reportEventId: EventID,
        name: String,
        type: CollectionType
    ) {
        runCatching {
            if (collection(name).exists()) {
                collection(name).drop()
            }
            createCollection(name, CollectionCreateOptions().type(type))
        }.onFailure { e ->
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to recreate $name:$type collection")
                    .type(EVENT_TYPE_INIT_DATABASE)
                    .status(Event.Status.FAILED)
                    .exception(e, true)
                    .toProto(reportEventId)
                    .log(K_LOGGER)
            )
            throw e
        }.onSuccess {
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Recreated $name:$type collection")
                    .type(EVENT_TYPE_INIT_DATABASE)
                    .toProto(reportEventId)
                    .log(K_LOGGER)
            )
        }.getOrThrow()
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
        private const val EVENT_TYPE_INIT_DATABASE: String = "Init Arango database"

        private fun Settings.createArangoDB() = ArangoDB.Builder()
                .host(arangoHost, arangoPort)
                .user(arangoUser)
                .password(arangoPassword)
                .build()

    }

}