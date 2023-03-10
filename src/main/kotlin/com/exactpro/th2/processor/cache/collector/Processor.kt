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
    private val persistor: Persistor
) : IProcessor {
    private val maxBatchSize: Int = settings.maxBatchSize
    private val maxFlushTime: Long = settings.maxFlushTime
    private val executor = Executors.newScheduledThreadPool(
        1,
        ThreadFactoryBuilder().setNameFormat("processor-cache-%d").build()
    )
    private val parsedMessageBatch = MessageBatcher(maxBatchSize, maxFlushTime, DIRECTION_SELECTOR, executor) {
        try {
            val grpcToParsedMessages =
                it.groupsList.map { group -> group.messagesList.map { el -> el.message.toCacheMessage() } }.flatten()
            persistor.insertParsedMessages(grpcToParsedMessages)
        } catch (e: Exception) {
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to insert parsed message")
                    .type(EVENT_TYPE_INSERT_PARSED_MESSAGE)
                    .status(Event.Status.FAILED)
                    .exception(e, true)
                    .toProto(processorEventId)
                    .log(K_LOGGER)
            )
        }
    }
    private val rawMessageBatch = RawMessageBatcher(maxBatchSize, maxFlushTime, RAW_DIRECTION_SELECTOR, executor) {
        try {
            val grpcToRawMessages =
                it.groupsList.map { group -> group.messagesList.map { el -> el.rawMessage.toCacheMessage() } }.flatten()
            persistor.insertRawMessages(grpcToRawMessages)
        } catch (e: Exception) {
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to insert raw message")
                    .type(EVENT_TYPE_INSERT_RAW_MESSAGE)
                    .status(Event.Status.FAILED)
                    .exception(e, true)
                    .toProto(processorEventId)
                    .log(K_LOGGER)
            )
        }
    }
    private val eventBatch = EventBatcher(maxBatchSize, maxFlushTime, executor) {
        try {
            val grpcToCacheEvents = it.eventsList.map { el -> el.toCacheEvent() }
            persistor.insertEvents(grpcToCacheEvents)
        } catch (e: Exception) {
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to insert event")
                    .type(EVENT_TYPE_INSERT_EVENT)
                    .status(Event.Status.FAILED)
                    .exception(e, true)
                    .toProto(processorEventId)
                    .log(K_LOGGER)
            )
        }
    }

    var errors = 0;
    override fun handle(intervalEventId: EventID, grpcEvent: GrpcEvent) {
        try {
             eventBatch.onEvent(grpcEvent)
        } catch (e: Exception) {
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to handle event")
                    .type(EVENT_TYPE_HANDLE_EVENT)
                    .status(Event.Status.FAILED)
                    .exception(e, true)
                    .toProto(processorEventId)
                    .log(K_LOGGER)
            )
            errors++
            K_LOGGER.error ( "Exception handling event ${grpcEvent.id.format()}, current number of errors = $errors", e )
        }
    }

    override fun handle(intervalEventId: EventID, grpcMessage: GrpcParsedMessage) {
        try {
            parsedMessageBatch.onMessage(grpcMessage.toBuilder())
        } catch (e: Exception) {
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to handle parsed message")
                    .type(EVENT_TYPE_HANDLE_PARSED_MESSAGE)
                    .status(Event.Status.FAILED)
                    .exception(e, true)
                    .toProto(processorEventId)
                    .log(K_LOGGER)
            )
            errors++
            K_LOGGER.error ( "Exception handling parsed message ${grpcMessage.id.format()}, current number of errors = $errors", e )
        }
    }

    override fun handle(intervalEventId: EventID, grpcMessage: GrpcRawMessage) {
        try {
            rawMessageBatch.onMessage(grpcMessage.toBuilder())
        } catch (e: Exception) {
            eventBatcher.onEvent(
                EventBuilder.start()
                    .name("Failed to handle raw message")
                    .type(EVENT_TYPE_HANDLE_RAW_MESSAGE)
                    .status(Event.Status.FAILED)
                    .exception(e, true)
                    .toProto(processorEventId)
                    .log(K_LOGGER)
            )
            errors++
            K_LOGGER.error ( "Exception handling raw message ${grpcMessage.id.format()}, current number of errors = $errors", e )
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
        const val EVENT_TYPE_INIT_DATABASE: String = "Init Arango database"
        const val EVENT_TYPE_HANDLE_EVENT: String = "Handle event"
        const val EVENT_TYPE_HANDLE_PARSED_MESSAGE: String = "Handle parsed message"
        const val EVENT_TYPE_HANDLE_RAW_MESSAGE: String = "Handle raw message"
        const val EVENT_TYPE_INSERT_EVENT: String = "Insert event"
        const val EVENT_TYPE_INSERT_PARSED_MESSAGE: String = "Insert parsed message"
        const val EVENT_TYPE_INSERT_RAW_MESSAGE: String = "Insert raw message"
    }
}
