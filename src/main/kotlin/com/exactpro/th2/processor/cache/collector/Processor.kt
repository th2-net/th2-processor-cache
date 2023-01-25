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

package com.exactpro.th2.processor.cache.collector

import com.arangodb.*
import com.arangodb.entity.BaseEdgeDocument
import com.arangodb.entity.CollectionType
import com.arangodb.entity.EdgeDefinition
import com.arangodb.model.CollectionCreateOptions
import com.exactpro.th2.cache.common.Arango
import com.exactpro.th2.cache.common.event.Event
import com.exactpro.th2.cache.common.message.ParsedMessage
import com.exactpro.th2.cache.common.message.RawMessage
import com.exactpro.th2.common.event.Event.Status
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.cache.collector.event.format
import com.exactpro.th2.processor.cache.collector.event.toCacheEvent
import com.exactpro.th2.processor.cache.collector.message.format
import com.exactpro.th2.processor.cache.collector.message.hasParentMessage
import com.exactpro.th2.processor.cache.collector.message.toCacheMessage
import com.exactpro.th2.processor.utility.log
import com.google.common.util.concurrent.ThreadFactoryBuilder
import mu.KotlinLogging
import java.util.concurrent.*

typealias GrpcEvent = com.exactpro.th2.common.grpc.Event
typealias GrpcParsedMessage = com.exactpro.th2.common.grpc.Message
typealias GrpcRawMessage = com.exactpro.th2.common.grpc.RawMessage
typealias EventBuilder = com.exactpro.th2.common.event.Event

class Processor(
    private val eventBatcher: EventBatcher,
    processorEventId: EventID,
    settings: Settings,
) : IProcessor {
    private val maxBatchSize: Int = settings.maxBatchSize
    private val maxFlushTime: Long = settings.maxFlushTime
    private val executor = Executors.newScheduledThreadPool(
        1,
        ThreadFactoryBuilder().setNameFormat("processor-cache-%d").build()
    )
    private val batch = EventBatcher(maxBatchSize, maxFlushTime, executor) {
        val map = it.eventsList.map { el -> el.toCacheEvent() }
        eventCollection.insertDocuments(map)
        map.filter { el -> el.parentEventId != null }.forEach { el -> storeEventRelationship(el) }
    }
    private val recreateCollections: Boolean = settings.recreateCollections
    private val arango: Arango = Arango(settings.arangoCredentials)
    private val database: ArangoDatabase = arango.getDatabase()
    private val rawMessageCollection: ArangoCollection
    private val parsedMessageCollection: ArangoCollection
    private val eventCollection: ArangoCollection
    private val eventRelationshipCollection: ArangoCollection
    private val parsedMessageRelationshipCollection: ArangoCollection

    init {
        createDB(processorEventId)

        initCollections(processorEventId, mapOf(
            Arango.EVENT_COLLECTION to CollectionType.DOCUMENT,
            Arango.RAW_MESSAGE_COLLECTION to CollectionType.DOCUMENT,
            Arango.PARSED_MESSAGE_COLLECTION to CollectionType.DOCUMENT,
            Arango.EVENT_EDGES to CollectionType.EDGES,
            Arango.MESSAGE_EDGES to CollectionType.EDGES
        ))

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

        eventRelationshipCollection = database.collection(Arango.EVENT_EDGES)
        parsedMessageRelationshipCollection = database.collection(Arango.MESSAGE_EDGES)
        eventCollection = database.collection(Arango.EVENT_COLLECTION)
        rawMessageCollection = database.collection(Arango.RAW_MESSAGE_COLLECTION)
        parsedMessageCollection = database.collection(Arango.PARSED_MESSAGE_COLLECTION)
    }

    var errors = 0;
    override fun handle(intervalEventId: EventID, grpcEvent: GrpcEvent) {
        try {
            batch.onEvent(grpcEvent)
//        if (event.attachedMessageIds !=null) {
//            event.attachedMessageIds?.forEach { messageId ->
//                // FIXME: maybe store as a batch
//                storeEdge(event, messageId)
//            }
//        }
        } catch (e: Exception) {
            errors++
            K_LOGGER.error ( "Exception handling event ${grpcEvent.id.format()}, current number of errors = $errors", e )
        }
    }

    override fun handle(intervalEventId: EventID, grpcMessage: GrpcParsedMessage) {
        try {
            var message = grpcMessage.toCacheMessage()
            storeDocument(message)
            if (message.hasParentMessage()) {
                storeMessageRelationship(message)
            }
        } catch (e: Exception) {
            errors++
            K_LOGGER.error ( "Exception handling event ${grpcMessage.id.format()}, current number of errors = $errors", e )
        }
    }

    override fun handle(intervalEventId: EventID, grpcMessage: GrpcRawMessage) {
        try {
            var message = grpcMessage.toCacheMessage()
            storeDocument(message)
        } catch (e: Exception) {
            errors++
            K_LOGGER.error ( "Exception handling event ${grpcMessage.id.format()}, current number of errors = $errors", e )
        }
    }

    private fun storeDocument(event: Event) {
        eventCollection.insertDocument(event)
    }

    private fun storeDocument(message: ParsedMessage) {
        parsedMessageCollection.insertDocument(message)
    }

    private fun storeDocument(message: RawMessage) {
        rawMessageCollection.insertDocument(message)
    }

    private fun storeMessageRelationship(message: ParsedMessage) {
        parsedMessageRelationshipCollection.insertDocument(BaseEdgeDocument().apply {
            from = getMessageKey(message.id.dropLast(2))
            to = getMessageKey(message.id)
        })
    }

    private fun storeEventRelationship(event: Event) {
//        This throws "Document not found exception" as not all vertexes of the edge are present in event collection
//        edgeCollection.insertEdge(BaseEdgeDocument().apply {
//            from = getEventKey(event.parentEventId!!)
//            to = getEventKey(event.eventId)
//        })

        // This way of creating edge works
        eventRelationshipCollection.insertDocument(BaseEdgeDocument().apply {
            from = getEventKey(event.parentEventId!!)
            to = getEventKey(event.eventId)
        })
    }

//    private fun storeEdge(event: Event, messageId: String) {
//        // FIXME: I'm not sure about using the same edge
//        edgeCollection.insertEdge(BaseEdgeDocument().apply {
//            from = event.eventId
//            to = messageId
//        })
//    }

    private fun getEventKey(eventId : String): String = Arango.EVENT_COLLECTION + "/" + eventId

    private fun getMessageKey(messageId: String): String = Arango.PARSED_MESSAGE_COLLECTION + "/" + messageId

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

    private fun initGraph(name: String, edgeDefinition: EdgeDefinition) {
        val graph = database.graph(name)
        var exists = graph.exists()
        if (exists && recreateCollections) {
            K_LOGGER.info { "Dropping graph \"${name}\"" }
            graph.drop()
        }
        exists = graph.exists()
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
                }
                exists = collection.exists()
                if (!exists) {
                    K_LOGGER.info { "Creating collection \"${name}\"" }
                    database.createCollection(name, CollectionCreateOptions().type(type))
                }
            }.onFailure { e ->
                eventBatcher.onEvent(
                    EventBuilder.start()
                        .name("Failed to create $name:$type collection")
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
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
        private const val EVENT_TYPE_INIT_DATABASE: String = "Init Arango database"
    }
}
