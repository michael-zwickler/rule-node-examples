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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.BasicTsKvEntry;
import org.thingsboard.server.common.data.kv.DoubleDataEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.msg.queue.TbMsgCallback;
import org.thingsboard.server.dao.timeseries.TimeseriesService;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class TbDuplicateMessageFilterTest {

    final ObjectMapper mapper = new ObjectMapper();

    EntityId entityId;
    TenantId tenantId;
    TbDuplicateMessageFilterConfiguration config;
    TbNodeConfiguration nodeConfiguration;
    TbDuplicateMessageFilter node;
    TbContext ctx;
    TbMsgCallback callback;
    TbMsgMetaData metaData;
    TimeseriesService timeseriesService;

    @BeforeEach
    void setUp() throws TbNodeException {
        entityId = new DeviceId(UUID.randomUUID());
        tenantId = new TenantId(UUID.randomUUID());

        config = new TbDuplicateMessageFilterConfiguration().defaultConfiguration();
        nodeConfiguration = new TbNodeConfiguration(mapper.valueToTree(config));

        callback = mock(TbMsgCallback.class);
        metaData = new TbMsgMetaData();
        timeseriesService = mock(TimeseriesService.class);

        ctx = mock(TbContext.class);
        when(ctx.getTimeseriesService()).thenReturn(timeseriesService);
        when(ctx.getTenantId()).thenReturn(tenantId);

        node = new TbDuplicateMessageFilter();
        node.init(ctx, nodeConfiguration);
    }

    @AfterEach
    void tearDown() {
        node.destroy();
    }

    @Test
    void givenDefaultConfig_whenInit_thenOK() {
        assertThat(node.maxTimeBetweenMessagesInMillis).isEqualTo(10_000L);
        assertThat(node.config).isEqualTo(config);
    }

    @Test
    void givenDefaultConfig_whenVerify_thenOK() {
        TbDuplicateMessageFilterConfiguration defaultConfig = new TbDuplicateMessageFilterConfiguration().defaultConfiguration();
        assertThat(defaultConfig.getMaxTimeBetweenMessagesInMillis()).isEqualTo(10_000L);
    }

    @Test
    void whenOnMsg_withTsOutsideBlockingPeriod_MsgShouldBePassedThrough() throws InterruptedException, ExecutionException {
        // Basic Test: Payload with one attribute, outside time blocking zone, should pass through

        String payload = "{\"temperature\":22.5}";
        TbMsg message = TbMsg.newMsg("POST_TELEMETRY_REQUEST", entityId, metaData, payload, callback);

        KvEntry kvEntry = new DoubleDataEntry("temperature", 25.7);
        TsKvEntry TsKvEntry = new BasicTsKvEntry(message.getTs() - 11_000L, kvEntry);
        List<TsKvEntry> entryList = List.of(TsKvEntry);
        SettableFuture<List<TsKvEntry>> settableFuture = SettableFuture.create();
        settableFuture.set(entryList);
        when(timeseriesService.findAllLatest(tenantId, entityId)).thenReturn(settableFuture);

        node.onMsg(ctx, message);

        verify(ctx, times(1)).tellNext(message, "True");
        verify(ctx, never()).tellNext(message, "False");
        verify(ctx, never()).tellFailure(any(), any());
    }

    @Test
    void whenOnMsg_withTsInsideBlockingPeriod_MsgShouldNotBePassedThrough() throws InterruptedException, ExecutionException {
        // Basic Test: Payload with one attribute, inside time blocking zone, should not pass through

        String payload = "{\"temperature\":22.5}";
        TbMsg message = TbMsg.newMsg("POST_TELEMETRY_REQUEST", entityId, metaData, payload, callback);

        KvEntry kvEntry = new DoubleDataEntry("temperature", 25.7);
        TsKvEntry TsKvEntry = new BasicTsKvEntry(message.getTs() - 9_000L, kvEntry);
        List<TsKvEntry> entryList = List.of(TsKvEntry);
        SettableFuture<List<TsKvEntry>> settableFuture = SettableFuture.create();
        settableFuture.set(entryList);
        when(timeseriesService.findAllLatest(tenantId, entityId)).thenReturn(settableFuture);

        node.onMsg(ctx, message);

        verify(ctx, never()).tellNext(message, "True");
        verify(ctx, times(1)).tellNext(message, "False");
        verify(ctx, never()).tellFailure(any(), any());
    }

    @Test
    void whenOnMsg_withTsInsideBlockingPeriodButAnOldAttributeIsInDatabase_MsgShouldNotBePassedThrough() throws InterruptedException, ExecutionException {
        // Corner Case: Payload with one attribute inside time blocking zone
        // Database entry with outside time blocking zone, which is not in payload
        // Should not pass through

        String payload = "{\"temperature\":22.5}";
        TbMsg message = TbMsg.newMsg("POST_TELEMETRY_REQUEST", entityId, metaData, payload, callback);

        KvEntry kvEntry1 = new DoubleDataEntry("temperature", 25.7);
        TsKvEntry TsKvEntry1 = new BasicTsKvEntry(message.getTs() - 9_000L, kvEntry1);
        KvEntry kvEntry2 = new DoubleDataEntry("humiditiy", 65.0);
        TsKvEntry TsKvEntry2 = new BasicTsKvEntry(message.getTs() - 11_000L, kvEntry2);

        List<TsKvEntry> entryList = List.of(TsKvEntry1, TsKvEntry2);
        SettableFuture<List<TsKvEntry>> settableFuture = SettableFuture.create();
        settableFuture.set(entryList);
        when(timeseriesService.findAllLatest(tenantId, entityId)).thenReturn(settableFuture);

        node.onMsg(ctx, message);

        verify(ctx, never()).tellNext(message, "True");
        verify(ctx, times(1)).tellNext(message, "False");
        verify(ctx, never()).tellFailure(any(), any());
    }

    @Test
    void whenOnMsg_withTsOutisdeBlockingPeriodButANewAttributeIsInDatabase_MsgShouldBePassedThrough() throws InterruptedException, ExecutionException {
        // Corner Case: Payload with one attribute outside time blocking zone
        // Database entry with an inside time blocking zone entry, which is not in payload
        // Should pass through

        String payload = "{\"temperature\":22.5}";
        TbMsg message = TbMsg.newMsg("POST_TELEMETRY_REQUEST", entityId, metaData, payload, callback);

        KvEntry kvEntry1 = new DoubleDataEntry("temperature", 25.7);
        TsKvEntry TsKvEntry1 = new BasicTsKvEntry(message.getTs() - 11_000L, kvEntry1);
        KvEntry kvEntry2 = new DoubleDataEntry("humiditiy", 65.0);
        TsKvEntry TsKvEntry2 = new BasicTsKvEntry(message.getTs() - 9_000L, kvEntry2);

        List<TsKvEntry> entryList = List.of(TsKvEntry1, TsKvEntry2);
        SettableFuture<List<TsKvEntry>> settableFuture = SettableFuture.create();
        settableFuture.set(entryList);
        when(timeseriesService.findAllLatest(tenantId, entityId)).thenReturn(settableFuture);

        node.onMsg(ctx, message);

        verify(ctx, times(1)).tellNext(message, "True");
        verify(ctx, never()).tellNext(message, "False");
        verify(ctx, never()).tellFailure(any(), any());
    }

    @Test
    void whenOnMsg_emptPayload_tellFailureShouldBeCalled() throws InterruptedException, ExecutionException {
        String payload = null;
        TbMsg message = TbMsg.newMsg("POST_TELEMETRY_REQUEST", entityId, metaData, payload, callback);

        node.onMsg(ctx, message);

        verify(ctx, never()).tellNext(any(),anyString());
        verify(ctx, times(1)).tellFailure(eq(message), any(Exception.class));
    }

    @Test
    void whenOnMsg_withNoDatabaseEntryYet_MsgShouldBePassedThrough() throws InterruptedException, ExecutionException {
        String payload = "{\"temperature\":22.5}";
        TbMsg message = TbMsg.newMsg("POST_TELEMETRY_REQUEST", entityId, metaData, payload, callback);

        List<TsKvEntry> emptyList = List.of();
        SettableFuture<List<TsKvEntry>> settableFuture = SettableFuture.create();
        settableFuture.set(emptyList);
        when(timeseriesService.findAllLatest(tenantId, entityId)).thenReturn(settableFuture);

        node.onMsg(ctx, message);

        verify(ctx, times(1)).tellNext(message, "True");
        verify(ctx, never()).tellNext(message, "False");
        verify(ctx, never()).tellFailure(any(), any());
    }

    @Test
    void whenOnMsg_whenPayloadContainsKeyThatIsNotYetInDatabase_MsgShouldBePassedThrough() throws InterruptedException, ExecutionException {
        // Create new incoming message
        String payload = "{\"temperature\":22.5, \"humudity\": 65.0}";
        TbMsg message = TbMsg.newMsg("POST_TELEMETRY_REQUEST", entityId, metaData, payload, callback);

        KvEntry kvEntry = new DoubleDataEntry("humiditiy", 65.0);
        TsKvEntry TsKvEntry = new BasicTsKvEntry(message.getTs() - 9_000L, kvEntry);
        List<TsKvEntry> entryList = List.of(TsKvEntry);
        SettableFuture<List<TsKvEntry>> settableFuture = SettableFuture.create();
        settableFuture.set(entryList);
        when(timeseriesService.findAllLatest(tenantId, entityId)).thenReturn(settableFuture);

        //execute method under test
        node.onMsg(ctx, message);

        // assertions
        verify(ctx, times(1)).tellNext(message, "True");
        verify(ctx, never()).tellNext(message, "False");
        verify(ctx, never()).tellFailure(any(), any());
    }

}
