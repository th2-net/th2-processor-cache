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

package com.exactpro.th2.processor.cache.collector.event

import com.exactpro.th2.cache.common.event.Event
import com.exactpro.th2.cache.common.toArangoTimestamp
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.common.utils.event.book
import com.exactpro.th2.common.utils.event.scope
import com.exactpro.th2.processor.cache.collector.GrpcEvent
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test


class TestCacheEvent {
    private val book = "book"
    private val scope = "scope"
    private val startTimestamp = Timestamp.newBuilder()
        .setSeconds(100)
        .setNanos(50)
        .build()
    private val eventId = EventID.newBuilder()
        .setId("eventId")
        .setBookName(book)
        .setScope(scope)
        .setStartTimestamp(startTimestamp)
        .build()
    private val parentEventId = EventID.newBuilder()
        .setId("2")
        .build()
    private val endTimestamp = Timestamp.newBuilder()
        .setSeconds(101)
        .setNanos(50)
        .build()
    private val status = EventStatus.SUCCESS
    private val name = "name"
    private val type = "type"
    private val body = ByteString.EMPTY
    private val connectionId = ConnectionID.newBuilder()
        .setSessionAlias("session-alias")
        .setSessionGroup("session-group")
        .build()
    private val messageId1 = MessageID.newBuilder()
        .setConnectionId(connectionId)
        .build()
    private val grpcEvent = GrpcEvent.newBuilder()
        .setId(eventId)
        .setParentId(parentEventId)
        .setEndTimestamp(endTimestamp)
        .setStatus(status)
        .setName(name)
        .setType(type)
        .setBody(body)
        .build()

    private fun compare(cacheEvent: Event) {
        assertEquals(cacheEvent.book, grpcEvent.book)
        assertEquals(cacheEvent.scope, grpcEvent.scope)
        assertEquals(cacheEvent.id, eventId.id)
        assertEquals(cacheEvent.eventName, grpcEvent.name)
        assertEquals(cacheEvent.eventType, grpcEvent.type)
        assertEquals(cacheEvent.startTimestamp, toArangoTimestamp(grpcEvent.id.startTimestamp.toInstant()))
        assertEquals(cacheEvent.endTimestamp, toArangoTimestamp(grpcEvent.endTimestamp.toInstant()))
        assertEquals(cacheEvent.successful, grpcEvent.isSuccess())
        assertEquals(cacheEvent.body, grpcEvent.body.toStringUtf8())
        assertEquals(cacheEvent.attachedMessageIds, grpcEvent.attachedMessageIdsList.map {  messageId -> messageId.toString() }.toSet())
    }

    @Test
    fun `formats event id correctly`() {
        assertEquals(eventId.format(), "book:scope:100:50:eventId")
    }

    @Test
    fun `converts grpc event to cache event`() {
        val cacheEvent = grpcEvent.toCacheEvent()
        compare(cacheEvent)
    }
}
