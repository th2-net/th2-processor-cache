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

package com.exactpro.th2.processor.cache.collecotor.event

import com.exactpro.th2.cache.common.event.Event
import com.exactpro.th2.cache.common.toArangoTimestamp
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.processor.cache.collecotor.GrpcEvent

internal fun GrpcEvent.toCacheEvent(): Event {

    return Event(
        eventId = id.format(),
        batchId = null,   // TODO: do we need batch id ?
        isBatched = false,  // TODO: do we need batch id ?
        eventName = name,
        eventType = type,
        startTimestamp = toArangoTimestamp(id.startTimestamp.toInstant()),
        endTimestamp = toArangoTimestamp(endTimestamp.toInstant()),

        parentEventId =
                            if (hasParentId()) {
                                parentId.format()
                            } else {
                                null
                            },
        successful = isSuccess(),
        attachedMessageIds =
                            if (attachedMessageIdsList != null) {
                                attachedMessageIdsList.map { messageId -> messageId.toString() }.toSet()
                            } else {
                                null
                            },
        body =
                            if (this.body != null) {
                                this.body.toStringUtf8()
                            } else {
                                null
                            }
    )
}

internal fun EventID.format(): String {

    val ts = startTimestamp.toInstant()
    return "${bookName}:${scope}:${ts.epochSecond}:${ts.nano}:${id}"
}

internal fun GrpcEvent.isSuccess(): Boolean {
    return when (status) {
        EventStatus.SUCCESS -> true
        EventStatus.FAILED -> false
        else -> throw IllegalArgumentException("Unknown event status '$status'")
    }
}
