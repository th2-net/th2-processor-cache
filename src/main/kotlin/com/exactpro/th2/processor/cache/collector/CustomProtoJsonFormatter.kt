/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/

package com.exactpro.th2.processor.cache.collector

import com.exactpro.th2.common.grpc.Value

class CustomProtoJsonFormatter : AbstractJsonFormatter() {

    companion object {
        internal const val QUOTE_CHAR = '"'.code
        internal const val BACK_SLASH = '\\'.code
    }

    override fun printV (value: Value, sb: StringBuilder) {
        when (value.kindCase) {
            Value.KindCase.SIMPLE_VALUE -> {
                sb.append("{\"simpleValue\":")
                convertStringToJson(value.simpleValue, sb)
                sb.append('}')
            }
            Value.KindCase.LIST_VALUE -> {
                sb.append("{\"listValue\":{\"values\":[")
                val valuesList = value.listValue.valuesList
                if (valuesList.isNotEmpty()) {
                    valuesList.forEach {
                        printV(it, sb)
                        sb.append(',')
                    }
                    sb.setLength(sb.length - 1)
                }
                sb.append("]}}")
            }
            Value.KindCase.MESSAGE_VALUE -> {
                sb.append("{\"messageValue\":")
                printM(value.messageValue, sb)
                sb.append('}')
            }
            Value.KindCase.NULL_VALUE -> {
                sb.append("{\"nullValue\":\"NULL_VALUE\"}")
            }
            else -> {
            }
        }
    }

}