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

import lombok.Data;
import org.thingsboard.rule.engine.api.NodeConfiguration;

@Data
public class TbDuplicateMessageFilterConfiguration implements NodeConfiguration<TbDuplicateMessageFilterConfiguration> {

    private static final long DEFAULT_MAX_TIME_BETWEEN_MESSAGES_IN_MILLIS = 10_000L;
    private long maxTimeBetweenMessagesInMillis;

    @Override
    public TbDuplicateMessageFilterConfiguration defaultConfiguration() {
        TbDuplicateMessageFilterConfiguration config = new TbDuplicateMessageFilterConfiguration();
        config.setMaxTimeBetweenMessagesInMillis(DEFAULT_MAX_TIME_BETWEEN_MESSAGES_IN_MILLIS);
        return config;
    }
}
