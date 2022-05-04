/**
 * Copyright © 2018 The Thingsboard Authors
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
package org.thingsboard.rule.engine.node.deduplicate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.Nullable;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.dao.timeseries.TimeseriesService;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

@RuleNode(
        type = ComponentType.FILTER,
        name = "Filter duplicate messages",
        relationTypes = {"True", "False"},
        configClazz = TbDuplicateMessageFilterConfiguration.class,
        nodeDescription = "Checks if a message only contains attributes, that are currently in block-out time, so has already pushed a message recently. If there is any new attribute or any attribute outside time block-out provided, the message will pass through",
        nodeDetails = "default block-out time: 10s",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbFilterNodeCheckKeyConfig"
)
public class TbDuplicateMessageFilter implements TbNode {

    private static final ObjectMapper mapper = new ObjectMapper();
    TbDuplicateMessageFilterConfiguration config;
    long maxTimeBetweenMessagesInMillis;
    TimeseriesService timeseriesService;

    @Override
    public void init(TbContext tbContext, TbNodeConfiguration tbNodeConfiguration) throws TbNodeException {
        this.config = TbNodeUtils.convert(tbNodeConfiguration, TbDuplicateMessageFilterConfiguration.class);
        maxTimeBetweenMessagesInMillis = config.getMaxTimeBetweenMessagesInMillis();
        timeseriesService = tbContext.getTimeseriesService();
    }

    @Override
    public void onMsg(TbContext tbContext, TbMsg tbMsg) throws ExecutionException, InterruptedException {
        TenantId tenantId = tbContext.getTenantId();
        EntityId entityId = tbMsg.getOriginator();
        JsonNode jsonMsgPayload = getJsonMsgPayload(tbContext, tbMsg);

        if (jsonMsgPayload == null)
            return;

        Futures.addCallback(timeseriesService.findAllLatest(tenantId, entityId), new FutureCallback<List<TsKvEntry>>() {
            @Override
            public void onSuccess(List<TsKvEntry> tsKvEntryList) {
                Iterator<String> payLoadAttributes = jsonMsgPayload.fieldNames();
                while (payLoadAttributes.hasNext()) {
                    String payloadAttribute = payLoadAttributes.next();
                    Optional<TsKvEntry> lastStoredTimeSeries = tsKvEntryList.stream()
                            .filter(tsKvEntry -> tsKvEntry.getKey().equals(payloadAttribute))
                            .findAny();

                    if (lastStoredTimeSeries.isEmpty()
                            || lastStoredTimeSeries.get().getTs() < tbMsg.getTs() - maxTimeBetweenMessagesInMillis) {
                        tbContext.tellNext(tbMsg, "True");
                        return;
                    }
                }
                tbContext.tellNext(tbMsg, "False");
            }

            @Override
            public void onFailure(Throwable throwable) {
                tbContext.tellFailure(tbMsg, throwable);
            }
        });

    }

    @Nullable
    private JsonNode getJsonMsgPayload(TbContext tbContext, TbMsg tbMsg) {
        JsonNode jsonMsgPayload;
        try {
            jsonMsgPayload = mapper.readTree(tbMsg.getData());
        } catch (JsonProcessingException | IllegalArgumentException e) {
            tbContext.tellFailure(tbMsg, e);
            return null;
        }
        return jsonMsgPayload;
    }

    @Override
    public void destroy() {
    }
}
