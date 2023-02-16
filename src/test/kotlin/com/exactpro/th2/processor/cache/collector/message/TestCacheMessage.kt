package com.exactpro.th2.processor.cache.collector.message

import com.exactpro.th2.cache.common.message.ParsedMessage
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.subsequence
import com.exactpro.th2.common.utils.message.direction
import com.exactpro.th2.common.utils.message.sessionAlias
import com.exactpro.th2.common.utils.message.sessionGroup
import com.exactpro.th2.processor.cache.collector.GrpcParsedMessage
import com.exactpro.th2.processor.cache.collector.GrpcRawMessage
import com.google.protobuf.Timestamp
import junit.framework.TestCase.assertTrue
import org.junit.Test

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


    private fun compare(cacheParsedMessage: ParsedMessage) {
        assert(cacheParsedMessage.book == messageId.bookName)
        assert(cacheParsedMessage.group == grpcMessage.sessionGroup)
        assert(cacheParsedMessage.sessionAlias == grpcMessage.sessionAlias)
        assert(cacheParsedMessage.direction == grpcMessage.direction.toString())
        assert(cacheParsedMessage.sequence == grpcMessage.sequence)
        assert(cacheParsedMessage.subsequence == grpcMessage.subsequence)
        assert(cacheParsedMessage.metadata.messageType == grpcMessage.metadata.messageType)
        assert(cacheParsedMessage.metadata.protocol == grpcMessage.metadata.protocol)
    }

    private fun compare(cacheRawMessage: com.exactpro.th2.cache.common.message.RawMessage) {
        assert(cacheRawMessage.book == messageId.bookName)
        assert(cacheRawMessage.group == grpcMessage.sessionGroup)
        assert(cacheRawMessage.sessionAlias == grpcMessage.sessionAlias)
        assert(cacheRawMessage.direction == grpcMessage.direction.toString())
        assert(cacheRawMessage.sequence == grpcMessage.sequence)
        assert(cacheRawMessage.metadata.protocol == grpcMessage.metadata.protocol)
    }

    @Test
    fun `formats parsed message id correctly`() {
        assertTrue(messageId.format() == "book:session-alias:1:100:1:1.2")
    }

    @Test
    fun `formats raw message id correctly`() {
        assertTrue(rawMessageId.format() == "book:session-alias:1:100:1")
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
}
