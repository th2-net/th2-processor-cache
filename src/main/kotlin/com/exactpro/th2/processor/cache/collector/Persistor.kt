/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.cache.common.event.Event
import com.exactpro.th2.cache.common.message.ParsedMessage
import com.exactpro.th2.cache.common.message.RawMessage

interface Persistor {
    fun prepareDatabase()
    fun insertEvents(events: List<Event>)
    fun insertParsedMessages(messages: List<ParsedMessage>)
    fun insertRawMessages(messages: List<RawMessage>)
}
