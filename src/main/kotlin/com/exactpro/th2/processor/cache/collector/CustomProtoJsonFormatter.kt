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

import com.exactpro.th2.common.grpc.Value

class CustomSimpleJsonFormatter : AbstractJsonFormatter() {
    override fun printV(value: Value, sb: StringBuilder) {
        when (value.kindCase) {
            Value.KindCase.NULL_VALUE -> sb.append("null")
            Value.KindCase.SIMPLE_VALUE -> convertStringToJson(value.simpleValue, sb)
            Value.KindCase.MESSAGE_VALUE -> printM(value.messageValue, sb)
            Value.KindCase.LIST_VALUE -> {
                sb.append("[")
                for ((count, element) in value.listValue.valuesList.withIndex()) {
                    if (count > 0) sb.append(',')
                    printV(element, sb)
                }
                sb.append(']')
            }
            Value.KindCase.KIND_NOT_SET, null -> error("unexpected kind ${value.kindCase}")
        }
    }
}
