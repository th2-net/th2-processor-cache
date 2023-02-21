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

package com.exactpro.th2.processor.cache.collector.message

import com.exactpro.th2.cache.common.message.ParsedMessage
import com.exactpro.th2.cache.common.toArangoTimestamp
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.subsequence
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.common.utils.message.direction
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.common.utils.message.sessionGroup
import com.exactpro.th2.common.utils.message.timestamp
import com.exactpro.th2.processor.cache.collector.CustomProtoJsonFormatter
import com.exactpro.th2.processor.cache.collector.GrpcParsedMessage
import com.exactpro.th2.processor.cache.collector.GrpcRawMessage
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestCacheMessage {
    private val book = "book"
    private val connectionId = ConnectionID.newBuilder()
        .setSessionAlias("session-alias")
        .setSessionGroup("session-group")
        .build()
    private val timestamp = Timestamp.newBuilder()
        .setSeconds(100)
        .setNanos(50)
        .build()
    private val direction = Direction.FIRST
    private val messageId = MessageID.newBuilder()
        .setBookName(book)
        .setTimestamp(timestamp)
        .setConnectionId(connectionId)
        .setDirection(direction)
        .setSequence(1)
        .addAllSubsequence(listOf(1, 2))
        .build()
    private val type = "type"
    private val metadata = MessageMetadata.newBuilder()
        .setId(messageId)
        .setMessageType(type)
        .build()
    private val parentEventId = EventID.newBuilder()
    private val grpcMessage = GrpcParsedMessage.newBuilder()
        .setParentEventId(parentEventId)
        .setMetadata(metadata)
        .addField("a", "1")
        .addField("b", "2")
        .build()
    private val rawMessageId = MessageID.newBuilder()
        .setBookName(book)
        .setTimestamp(timestamp)
        .setConnectionId(connectionId)
        .setDirection(direction)
        .setSequence(1)
        .build()
    private val rawMessageMetadata = RawMessageMetadata.newBuilder()
        .setId(rawMessageId)
        .build()
    private val grpcRawMessage = GrpcRawMessage.newBuilder()
        .setMetadata(rawMessageMetadata)
        .build()

    private fun GrpcParsedMessage.getBody(): Map<String, Any> {
        val jsonString = CustomProtoJsonFormatter().print(this)
        return ObjectMapper().readValue(jsonString, Map::class.java) as Map<String, Any>
    }

    private fun compare(cacheParsedMessage: ParsedMessage) {
        assertEquals(cacheParsedMessage.book, messageId.bookName)
        assertEquals(cacheParsedMessage.group, grpcMessage.sessionGroup)
        assertEquals(cacheParsedMessage.sessionAlias, grpcMessage.sessionAlias)
        assertEquals(cacheParsedMessage.direction, grpcMessage.direction.toString())
        assertEquals(cacheParsedMessage.sequence, grpcMessage.sequence)
        assertEquals(cacheParsedMessage.subsequence, grpcMessage.subsequence)
        assertEquals(cacheParsedMessage.timestamp, toArangoTimestamp(grpcMessage.timestamp.toInstant()))
        assertEquals(cacheParsedMessage.body, grpcMessage.getBody())
        assertEquals(cacheParsedMessage.metadata.messageType, grpcMessage.metadata.messageType)
        assertEquals(cacheParsedMessage.metadata.protocol, grpcMessage.metadata.protocol)
        assertEquals(cacheParsedMessage.metadata.properties, grpcMessage.metadata.propertiesMap)
    }

    private fun compare(cacheRawMessage: com.exactpro.th2.cache.common.message.RawMessage) {
        assertEquals(cacheRawMessage.book, messageId.bookName)
        assertEquals(cacheRawMessage.group, grpcRawMessage.sessionGroup)
        assertEquals(cacheRawMessage.sessionAlias, grpcRawMessage.sessionAlias)
        assertEquals(cacheRawMessage.direction, grpcRawMessage.direction.toString())
        assertEquals(cacheRawMessage.sequence, grpcRawMessage.sequence)
        assertEquals(cacheRawMessage.timestamp, toArangoTimestamp(grpcRawMessage.timestamp.toInstant()))
        assertEquals(cacheRawMessage.body, grpcRawMessage.body.toByteArray())
        assertEquals(cacheRawMessage.metadata.protocol, grpcRawMessage.metadata.protocol)
        assertEquals(cacheRawMessage.metadata.properties, grpcRawMessage.metadata.propertiesMap)
    }

    @Test
    fun `formats parsed message id correctly`() {
        assertEquals(messageId.format(), "book:session-alias:1:100:1:1.2")
    }

    @Test
    fun `formats raw message id correctly`() {
        assertEquals(rawMessageId.format(), "book:session-alias:1:100:1")
    }

    @Test
    fun `converts grpc parsed message to cache parsed message`() {
        val cacheParsedMessage = grpcMessage.toCacheMessage()
        compare(cacheParsedMessage)
    }
    
    @Test
    fun `converts grpc raw message to cache raw message`() {
        val cacheRawMessage = grpcRawMessage.toCacheMessage()
        compare(cacheRawMessage)
    }

    @Test
    fun `generates json correctly`() {
        val json = CustomProtoJsonFormatter().print(grpcMessage)
        assertEquals(json, """{"a":"1","b":"2"}""")
    }
}
