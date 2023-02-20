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

import com.arangodb.entity.CollectionType
import com.arangodb.entity.EdgeDefinition
import com.exactpro.th2.cache.common.Arango
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.*
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.cache.collector.event.format
import com.exactpro.th2.processor.cache.collector.event.toCacheEvent
import com.exactpro.th2.processor.cache.collector.message.format
import com.exactpro.th2.processor.cache.collector.message.toCacheMessage
import com.exactpro.th2.processor.utility.log
import com.google.common.util.concurrent.ThreadFactoryBuilder
import mu.KotlinLogging
import java.util.concurrent.Executors

typealias GrpcEvent = com.exactpro.th2.common.grpc.Event
typealias GrpcParsedMessage = com.exactpro.th2.common.grpc.Message
typealias GrpcRawMessage = com.exactpro.th2.common.grpc.RawMessage
typealias EventBuilder = com.exactpro.th2.common.event.Event

class Processor(
    private val eventBatcher: EventBatcher,
    private val processorEventId: EventID,
    settings: Settings,
    private val arangoDB: ArangoDB
) : IProcessor {
    private val maxBatchSize: Int = settings.maxBatchSize
    private val maxFlushTime: Long = settings.maxFlushTime
    private val executor = Executors.newScheduledThreadPool(
        1,
        ThreadFactoryBuilder().setNameFormat("processor-cache-%d").build()
    )
    private val parsedMessageBatch = MessageBatcher(maxBatchSize, maxFlushTime, DIRECTION_SELECTOR, executor) {
        val grpcToParsedMessages = it.groupsList.map { group -> group.messagesList.map { el -> el.message.toCacheMessage() } }.flatten()
        arangoDB.insertParsedMessages(grpcToParsedMessages)
    }
    private val rawMessageBatch = RawMessageBatcher(maxBatchSize, maxFlushTime, RAW_DIRECTION_SELECTOR, executor) {
        val grpcToRawMessages = it.groupsList.map { group -> group.messagesList.map { el -> el.rawMessage.toCacheMessage() } }.flatten()
        arangoDB.insertRawMessages(grpcToRawMessages)
    }
    private val eventBatch = EventBatcher(maxBatchSize, maxFlushTime, executor) {
        val grpcToCacheEvents = it.eventsList.map { el -> el.toCacheEvent() }
        arangoDB.insertEvents(grpcToCacheEvents)
    }

    internal fun init() {
        try {
            arangoDB.createDB()
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Created ${arangoDB.database.dbName()} database")
                    .type(ArangoDB.EVENT_TYPE_INIT_DATABASE)
                    .toProto(processorEventId)
                    .log(ArangoDB.K_LOGGER)
            )
        } catch (e: Exception) {
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to create ${arangoDB.database.dbName()} database")
                    .type(ArangoDB.EVENT_TYPE_INIT_DATABASE)
                    .status(Event.Status.FAILED)
                    .exception(e, true)
                    .toProto(processorEventId)
                    .log(ArangoDB.K_LOGGER)
            )
        }
        arangoDB.initCollections(processorEventId)
        arangoDB.initGraphs()

        arangoDB.eventRelationshipCollection = arangoDB.database.collection(Arango.EVENT_EDGES)
        arangoDB.parsedMessageRelationshipCollection = arangoDB.database.collection(Arango.MESSAGE_EDGES)
        arangoDB.eventCollection = arangoDB.database.collection(Arango.EVENT_COLLECTION)
        arangoDB.rawMessageCollection = arangoDB.database.collection(Arango.RAW_MESSAGE_COLLECTION)
        arangoDB.parsedMessageCollection = arangoDB.database.collection(Arango.PARSED_MESSAGE_COLLECTION)
    }

    var errors = 0;
    override fun handle(intervalEventId: EventID, grpcEvent: GrpcEvent) {
        try {
             eventBatch.onEvent(grpcEvent)
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
            parsedMessageBatch.onMessage(grpcMessage.toBuilder())
        } catch (e: Exception) {
            errors++
            K_LOGGER.error ( "Exception handling parsed message ${grpcMessage.id.format()}, current number of errors = $errors", e )
        }
    }

    override fun handle(intervalEventId: EventID, grpcMessage: GrpcRawMessage) {
        try {
            rawMessageBatch.onMessage(grpcMessage.toBuilder())
        } catch (e: Exception) {
            errors++
            K_LOGGER.error ( "Exception handling raw message ${grpcMessage.id.format()}, current number of errors = $errors", e )
        }
    }

    companion object {
        val K_LOGGER = KotlinLogging.logger {}
    }
}
