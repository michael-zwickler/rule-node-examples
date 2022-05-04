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
import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@RuleNode(
        type = ComponentType.FILTER,
        name = "Filter duplicate messages",
        relationTypes = {"True", "False"},
        configClazz = TbDuplicateMessageFilterConfiguration.class,
        nodeDescription = "Checks if the supplied values are similiar to ones recently submitted",
        nodeDetails = "filters",
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
        long msgTs = tbMsg.getTs();
        JsonNode jsonMsgPayload;

        try {
            jsonMsgPayload = mapper.readTree(tbMsg.getData());
        } catch (JsonProcessingException | IllegalArgumentException e) {
            tbContext.tellFailure(tbMsg, e);
            return;
        }

        // get latest timeseries data to see if any value in there is 'old'
        ListenableFuture<List<TsKvEntry>> requestLatestTimeseries = timeseriesService.findAllLatest(tenantId, entityId);
        List<TsKvEntry> latestTimeseries = requestLatestTimeseries.get();

        if (latestTimeseries.isEmpty()) {
            tbContext.tellNext(tbMsg, "True");
            return;
        }

        Iterator<String> payLoadAttributes = jsonMsgPayload.fieldNames();
        while (payLoadAttributes.hasNext()) {
            String payloadAttribute = payLoadAttributes.next();

            if (latestTimeseries.stream()
                    .noneMatch(tsKvEntry -> tsKvEntry.getKey().equals(payloadAttribute))) {
                tbContext.tellNext(tbMsg, "True");
                return;
            }

            if (latestTimeseries.stream()
                    .filter(tsKvEntry -> tsKvEntry.getKey().equals(payloadAttribute))
                    .anyMatch(tsKvEntry -> tsKvEntry.getTs() < tbMsg.getTs() - maxTimeBetweenMessagesInMillis)) {
                tbContext.tellNext(tbMsg, "True");
                return;
            }
        }
        tbContext.tellNext(tbMsg, "False");
    }

    @Override
    public void destroy() {
    }
}
