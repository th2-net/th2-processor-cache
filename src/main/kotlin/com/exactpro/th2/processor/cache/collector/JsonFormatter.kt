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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value

class JsonFormatter {
    val map = mutableMapOf<String, Any>()

    internal fun extractBody(message: Message): MutableMap<String, Any> {
        getMessage(message)
        return map
    }

    private fun getMessage(msg: Message) {
        val fieldsMap = msg.fieldsMap
        if (fieldsMap.isNotEmpty()) {
            for (entry in fieldsMap.entries) {
                map[entry.key] = getValue(entry.value)
            }
        }
    }

    private fun getValue(value: Value): Any {
        return when (value.kindCase) {
            Value.KindCase.NULL_VALUE -> "null"
            Value.KindCase.SIMPLE_VALUE -> value.simpleValue
            Value.KindCase.MESSAGE_VALUE -> getMessageMap(value.messageValue)
            Value.KindCase.LIST_VALUE -> value.listValue.valuesList.map { getValue(it) }
            Value.KindCase.KIND_NOT_SET, null -> error("unexpected kind ${value.kindCase}")
        }
    }

    private fun getMessageMap(msg: Message): Any {
        val fieldsMap = msg.fieldsMap
        if (fieldsMap.isNotEmpty()) {
            return fieldsMap.entries.associate { it.key to getValue(it.value) }
        }
        return ""
    }
}
