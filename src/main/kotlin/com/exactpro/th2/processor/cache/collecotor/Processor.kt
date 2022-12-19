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

import com.arangodb.ArangoDatabase
import com.arangodb.ArangoEdgeCollection
import com.arangodb.ArangoVertexCollection
import com.arangodb.entity.BaseEdgeDocument
import com.arangodb.entity.CollectionType
import com.arangodb.entity.EdgeDefinition
import com.arangodb.model.CollectionCreateOptions
import com.exactpro.th2.cache.common.Arango
import com.exactpro.th2.cache.common.event.Event
import com.exactpro.th2.common.event.Event.Status
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.cache.collecotor.event.toCacheEvent
import com.exactpro.th2.processor.cache.collecotor.message.CacheMessage.Companion.toCacheMessage
import com.exactpro.th2.processor.utility.log
import mu.KotlinLogging

typealias GrpcEvent = com.exactpro.th2.common.grpc.Event
typealias GrpcMessage = Message
typealias EventBuilder = com.exactpro.th2.common.event.Event

class Processor(
    private val eventBatcher: EventBatcher,
    processorEventId: EventID,
    settings: Settings,
) : IProcessor {


    private val arango: Arango = Arango(settings.arangoCredentials)
    private val database: ArangoDatabase = arango.getDatabase()
    private val rawMessageVertexCollection: ArangoVertexCollection
    private val parsedMessageVertexCollection: ArangoVertexCollection
    private val eventVertexCollection: ArangoVertexCollection
    private val edgeVertexCollection: ArangoEdgeCollection

    init {
        createDB(processorEventId)

        recreateCollection(processorEventId, Arango.EVENT_COLLECTION, CollectionType.DOCUMENT)
        recreateCollection(processorEventId, Arango.RAW_MESSAGE_COLLECTION, CollectionType.DOCUMENT)
        recreateCollection(processorEventId, Arango.PARSED_MESSAGE_COLLECTION, CollectionType.DOCUMENT)
        recreateCollection(processorEventId, Arango.EVENT_EDGES, CollectionType.EDGES)

        val edgeDefinition: EdgeDefinition = EdgeDefinition()
            .collection(Arango.EVENT_EDGES)
            .from(Arango.EVENT_COLLECTION)
            .to(Arango.EVENT_COLLECTION)

        recreateGraph(edgeDefinition)

        with(database.graph(Arango.EVENT_GRAPH)) {
            edgeVertexCollection = edgeCollection(Arango.EVENT_EDGES)
            eventVertexCollection = vertexCollection(Arango.EVENT_COLLECTION)
            rawMessageVertexCollection = vertexCollection(Arango.RAW_MESSAGE_COLLECTION)
            parsedMessageVertexCollection = vertexCollection(Arango.PARSED_MESSAGE_COLLECTION)
        }
    }

    override fun handle(intervalEventId: EventID, grpcEvent: GrpcEvent) {
        var event = grpcEvent.toCacheEvent()
        with (grpcEvent) {
            K_LOGGER.info { id.bookName }
            K_LOGGER.info { id.scope }
            K_LOGGER.info { id.id }
            K_LOGGER.info { id.startTimestamp.toString() }
        }
        K_LOGGER.info ( event.eventId )
        storeDocument(event)
        if (grpcEvent.hasParentId()) {
            storeEdge(event)
        }
//        if (event.attachedMessageIds !=null) {
//            event.attachedMessageIds?.forEach { messageId ->
//                // FIXME: maybe store as a batch
//                storeEdge(event, messageId)
//            }
//        }
    }

    override fun handle(intervalEventId: EventID, message: Message) {
        storeDocument(message)
    }

    private fun storeDocument(event: Event) {
        eventVertexCollection.insertVertex(event)
    }

    private fun storeDocument(message: GrpcMessage) {
        rawMessageVertexCollection.insertVertex(message.toCacheMessage())
    }

    private fun storeEdge(event: Event) {
        edgeVertexCollection.insertEdge(BaseEdgeDocument().apply {
            from = event.parentEventId
            to = event.eventId
        })
    }

//    private fun storeEdge(event: Event, messageId: String) {
//        // FIXME: I'm not sure about using the same edge
//        edgeVertexCollection.insertEdge(BaseEdgeDocument().apply {
//            from = event.eventId
//            to = messageId
//        })
//    }

    private fun createDB(
        reportEventId: EventID,
    ) {
        runCatching {
            if (!database.exists()) {
                database.create()
                eventBatcher.onEvent(
                    EventBuilder.start()
                        .name("Created ${database.dbName()} database")
                        .type(EVENT_TYPE_INIT_DATABASE)
                        .toProto(reportEventId)
                        .log(K_LOGGER)
                )
            }
        }.onFailure { e ->
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to create ${database.dbName()} database")
                    .type(EVENT_TYPE_INIT_DATABASE)
                    .status(Status.FAILED)
                    .exception(e, true)
                    .toProto(reportEventId)
                    .log(K_LOGGER)
            )
            throw e
        }.getOrThrow()
    }

    private fun recreateGraph(edgeDefinition: EdgeDefinition) {
        if (database.graph(Arango.EVENT_GRAPH).exists()) {
            database.graph(Arango.EVENT_GRAPH).drop()
        }
        database.createGraph(Arango.EVENT_GRAPH, mutableListOf(edgeDefinition), null)
    }

    private fun recreateCollection(
        reportEventId: EventID,
        name: String,
        type: CollectionType
    ) {
        runCatching {
            if (database.collection(name).exists()) {
                database.collection(name).drop()
            }
            database.createCollection(name, CollectionCreateOptions().type(type))
        }.onFailure { e ->
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to recreate $name:$type collection")
                    .type(EVENT_TYPE_INIT_DATABASE)
                    .status(Status.FAILED)
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
    }
}