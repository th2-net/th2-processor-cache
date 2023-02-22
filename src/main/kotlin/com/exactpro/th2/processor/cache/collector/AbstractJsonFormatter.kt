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

interface JsonFormatter {
    fun print(message: Message): Map<String, Any>
}

abstract class AbstractJsonFormatter : JsonFormatter {
    val map = mutableMapOf<String, Any>()

    override fun print(message: Message): MutableMap<String, Any> {
        printM(message, map)
        return map
    }

    protected abstract fun printV(value: Value): Any

    protected fun printM (msg: Message, map: MutableMap<String, Any>) {
        val fieldsMap = msg.fieldsMap
        if (fieldsMap.isNotEmpty()) {
            for (entry in fieldsMap.entries) {
                map[entry.key] = printV(entry.value)
            }
        }
    }
}
