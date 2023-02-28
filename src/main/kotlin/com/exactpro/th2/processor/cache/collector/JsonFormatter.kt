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

    internal fun print(message: Message): MutableMap<String, Any> {
        printM(message)
        return map
    }

    private fun printM(msg: Message) {
        val fieldsMap = msg.fieldsMap
        if (fieldsMap.isNotEmpty()) {
            for (entry in fieldsMap.entries) {
                map[entry.key] = printV(entry.value)
            }
        }
    }

    private fun printV(value: Value): Any {
        return when (value.kindCase) {
            Value.KindCase.NULL_VALUE -> "null"
            Value.KindCase.SIMPLE_VALUE -> value.simpleValue
            Value.KindCase.MESSAGE_VALUE -> f(value.messageValue)
            Value.KindCase.LIST_VALUE -> value.listValue.valuesList.map { printV(it) }
            Value.KindCase.KIND_NOT_SET, null -> error("unexpected kind ${value.kindCase}")
        }
    }

    private fun f(msg: Message): Any {
        val fieldsMap = msg.fieldsMap
        if (fieldsMap.isNotEmpty()) {
            return fieldsMap.entries.associate { it.key to printV(it.value) }
        }
        return ""
    }
}
