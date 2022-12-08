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

package com.exactpro.th2.processor.cache.collecotor

import com.exactpro.th2.processor.api.IProcessorSettings

data class Settings(
    val arangoHost: String = "localhost",
    val arangoPort: Int = 8529,
    val arangoUser: String = "",
    val arangoPassword: String = "",
    val arangoDbName: String = "",

    val eventHierarchyGraph: String = "Event_hierarchy_graph",
    val edgeCollection: String? = "Edges", //TODO: maybe it is not nullable ?
    val messageCollection: String? = "Messages", //TODO: maybe it is not nullable ?
    val eventCollection: String? = "Events", //TODO: maybe it is not nullable ?
): IProcessorSettings