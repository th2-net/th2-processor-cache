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
import java.time.Instant

class TestProcessor {
    private lateinit var processor: Processor
    private val eventBatcherMock = Mockito.mock(EventBatcher::class.java)
    private val processorEventIdMock = Mockito.mock(EventID::class.java)
    private val arangoCredentialsMock = Mockito.mock(ArangoCredentials::class.java)
    private val settings = Settings(arangoCredentialsMock)
    private val arangoDb = Mockito.mock(ArangoDB::class.java)

    @BeforeEach
    fun init() {
        processor = Processor(eventBatcherMock, processorEventIdMock, settings, arangoDb)
    }

    @Test
    fun callsInsertEvents() {
        val list = mutableListOf<GrpcEvent>()
        for (i in 1..100) {
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

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        Mockito.verify(arangoDb, Mockito.times(1)).insertEvents(list.map { it.toCacheEvent() })
    }

    @Test
    fun callsInsertRawMessages() {
        val list = mutableListOf<GrpcRawMessage>()
        for (i in 1..100) {
            val grpcRawMessage = GrpcRawMessage.newBuilder()
                .build()
            list.add(grpcRawMessage)
        }

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        Mockito.verify(arangoDb, Mockito.times(1)).insertRawMessages(MockitoHelper.anyObject())
    }

    @Test
    fun callsInsertParsedMessages() {
        val list = mutableListOf<GrpcParsedMessage>()
        for (i in 1..100) {
            val grpcParsedMessage = GrpcParsedMessage.newBuilder()
                .build()
            list.add(grpcParsedMessage)
        }

        list.forEach { processor.handle(INTERVAL_EVENT_ID, it) }
        Mockito.verify(arangoDb, Mockito.times(1)).insertParsedMessages(MockitoHelper.anyObject())
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
