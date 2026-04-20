/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.Set;

/**
 * A data format that tracks document deletions via live docs bitsets (.liv files).
 * <p>
 * This format is automatically associated by the engine when no active data format
 * {@linkplain DataFormat#handlesDeletesNatively() handles deletes natively}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DeleteDataFormat extends DataFormat {

    public static final String NAME = "delete";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public long priority() {
        return Long.MAX_VALUE;
    }

    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return Collections.emptySet();
    }
}
