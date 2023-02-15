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

package com.exactpro.th2.processor.cache.collector.message

import com.exactpro.th2.cache.common.message.ParsedMessage
import com.exactpro.th2.cache.common.message.ParsedMessageMetadata
import com.exactpro.th2.cache.common.message.RawMessage
import com.exactpro.th2.cache.common.message.RawMessageMetadata
import com.exactpro.th2.cache.common.toArangoTimestamp
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.*
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.processor.cache.collector.CustomProtoJsonFormatter
import com.exactpro.th2.processor.cache.collector.GrpcParsedMessage
import com.exactpro.th2.processor.cache.collector.GrpcRawMessage
import com.fasterxml.jackson.databind.ObjectMapper


//FIXME: Vertex id should be generated base on the message id
internal fun GrpcParsedMessage.toCacheMessage(): ParsedMessage {
    val jsonString = CustomProtoJsonFormatter().print(this)
    val map = ObjectMapper().readValue(jsonString, Map::class.java) as Map<String, Any>
    return ParsedMessage(
        id = id.format(),
        book = id.bookName,
        group = id.connectionId.sessionGroup,
        sessionAlias = id.connectionId.sessionAlias,
        direction = direction.toString(),
        sequence = id.sequence,
        subsequence = id.subsequenceList,
        timestamp = toArangoTimestamp(id.timestamp.toInstant()),
        attachedEventIds = emptySet(),
        body = map,
        metadata = metadata.toParsedMessageMetadata()
    )
}

internal fun GrpcRawMessage.toCacheMessage(): RawMessage {
    return RawMessage(
        id = id.format(),
        book = id.bookName,
        group = id.connectionId.sessionGroup,
        sessionAlias = id.connectionId.sessionAlias,
        direction = direction.toString(),
        sequence = id.sequence,
        timestamp = toArangoTimestamp(id.timestamp.toInstant()),
        attachedEventIds = emptySet(),
        body = body.toByteArray(),
        metadata = metadata.toRawMessageMetadata()
    )
}

internal fun MessageID.format(): String {
    val ts = timestamp.toInstant().epochSecond
    val dr = if (direction.toString() == "FIRST") 1 else 2
    if (subsequenceList.isEmpty()) {
        return "${bookName}:${connectionId.sessionAlias}:$dr:${ts}:${sequence}"
    }
    return "${bookName}:${connectionId.sessionAlias}:$dr:${ts}:${sequence}:${subsequenceList.joinToString { "." }}"
}

internal fun com.exactpro.th2.common.grpc.MessageMetadata.toParsedMessageMetadata() : ParsedMessageMetadata {
    return ParsedMessageMetadata(id.bookName, toArangoTimestamp(id.timestamp.toInstant()), messageType, propertiesMap, protocol)
}

internal fun com.exactpro.th2.common.grpc.RawMessageMetadata.toRawMessageMetadata() : RawMessageMetadata {
    return RawMessageMetadata(id.bookName, toArangoTimestamp(id.timestamp.toInstant()), propertiesMap, protocol)
}

internal fun ParsedMessage.hasParentMessage(): Boolean {
    return subsequence.size > 1
}

internal fun ParsedMessage.getParentMessageId(): String {
    return "${book}:${sessionAlias}:${timestamp}:${sequence}:${subsequence.dropLast(1).joinToString { "." }}"
}
