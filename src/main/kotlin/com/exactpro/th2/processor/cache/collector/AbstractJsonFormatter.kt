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
import com.google.gson.Gson

interface JsonFormatter {
    fun print(message: Message): String
}

abstract class AbstractJsonFormatter : JsonFormatter {
    private val sb1: StringBuilder = StringBuilder()
    private val gson = Gson()
    override fun print(message: Message): String {
        sb1.setLength(0)
        printM(message, sb1)
        return sb1.toString()
    }

    protected abstract fun printV(value: Value, sb: StringBuilder)

    protected fun printM (msg: Message, sb: StringBuilder) {
        sb.append("{")
        val fieldsMap = msg.fieldsMap
        if (fieldsMap.isNotEmpty()) {
            for (entry in fieldsMap.entries) {
                sb.append('"').append(entry.key).append("\":")
                printV(entry.value, sb)
                sb.append(',')
            }
            sb.setLength(sb.length - 1)
        }
        sb.append('}')
    }

    private fun isNeedToEscape(s: String): Boolean {
        // ascii 32 is space, all chars below should be escaped
        return s.chars().anyMatch { it < 32 || it == CustomProtoJsonFormatter.QUOTE_CHAR || it == CustomProtoJsonFormatter.BACK_SLASH }
    }

    protected fun convertStringToJson(s: String, builder: StringBuilder) {
        if (isNeedToEscape(s)) {
            gson.toJson(s, builder)
        } else {
            builder.append('"').append(s).append('"')
        }
    }
}
