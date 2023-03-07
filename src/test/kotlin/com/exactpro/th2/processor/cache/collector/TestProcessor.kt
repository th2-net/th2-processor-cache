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

import com.exactpro.th2.cache.common.ArangoCredentials
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.processor.cache.collector.event.toCacheEvent
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.after
import org.mockito.Mockito.times
import java.time.Instant

class TestProcessor {
    private lateinit var processor: Processor
    private val eventBatcherMock = Mockito.mock(EventBatcher::class.java)
    private val processorEventIdMock = Mockito.mock(EventID::class.java)
    private val arangoCredentialsMock = Mockito.mock(ArangoCredentials::class.java)
    private val settings = Settings(arangoCredentialsMock)
    private val arangoPersistor = Mockito.mock(ArangoPersister::class.java)

    @BeforeEach
    fun init() {
        processor = Processor(eventBatcherMock, processorEventIdMock, settings, arangoPersistor)
    }

    private fun generateEvents(size: Int): MutableList<GrpcEvent> {
        val list = mutableListOf<GrpcEvent>()
        for (i in 1..size) {
            val grpcEvent = GrpcEvent.newBuilder()
                .setId(EVENT_ID)
                .setEndTimestamp(Timestamp.newBuilder().build())
                .setStatus(EventStatus.SUCCESS)
                .setName("name")
                .setType("type")
                .setBody(ByteString.EMPTY)
                .build()
            list.add(grpcEvent)
        }
        return list
    }

    private fun generateRawMessages(size: Int): MutableList<GrpcRawMessage> {
        val list = mutableListOf<GrpcRawMessage>()
        for (i in 1..size) {
            val grpcRawMessage = GrpcRawMessage.newBuilder()
                .build()
            list.add(grpcRawMessage)
        }
        return list
    }

    private fun generateParsedMessages(size: Int): MutableList<GrpcParsedMessage> {
        val list = mutableListOf<GrpcParsedMessage>()
        for (i in 1..size) {
            val grpcParsedMessage = GrpcParsedMessage.newBuilder()
                .build()
            list.add(grpcParsedMessage)
        }
        return list
    }

    @Test
    fun callsInsertEvents() {
        val list = generateEvents(100)

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        Mockito.verify(arangoPersistor, times(1)).insertEvents(list.map { it.toCacheEvent() })
    }

    @Test
    fun callsInsertEventsMultipleTimes() {
        val list = generateEvents(130)

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        Mockito.verify(arangoPersistor, after(1100).times(2)).insertEvents(MockitoHelper.anyObject())
    }

    @Test
    fun callsInsertRawMessages() {
        val list = generateRawMessages(100)

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        Mockito.verify(arangoPersistor, times(1)).insertRawMessages(MockitoHelper.anyObject())
    }

    @Test
    fun callsInsertRawMessagesMultipleTimes() {
        val list = generateRawMessages(130)

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        Mockito.verify(arangoPersistor, after(1100).times(2)).insertRawMessages(MockitoHelper.anyObject())
    }

    @Test
    fun callsInsertParsedMessages() {
        val list = generateParsedMessages(100)

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        Mockito.verify(arangoPersistor, times(1)).insertParsedMessages(MockitoHelper.anyObject())
    }

    @Test
    fun callsInsertParsedMessagesMultipleTimes() {
        val list = generateParsedMessages(130)

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        Mockito.verify(arangoPersistor, after(1100).times(2)).insertParsedMessages(MockitoHelper.anyObject())
    }

    object MockitoHelper {
        fun <T> anyObject(): T {
            Mockito.any<T>()
            return uninitialized()
        }
        @Suppress("UNCHECKED_CAST")
        fun <T> uninitialized(): T = null as T
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
