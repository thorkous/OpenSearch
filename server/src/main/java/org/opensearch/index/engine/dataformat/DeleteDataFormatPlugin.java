/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.plugins.Plugin;

/**
 * Plugin interface for providing a {@link DeleteDataFormat} implementation.
 * <p>
 * Discovered via {@link org.opensearch.plugins.PluginsService#filterPlugins(Class)}.
 * Only one implementation should be registered; if multiple are found, the registry
 * will throw an {@link IllegalStateException}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DeleteDataFormatPlugin extends Plugin {

    private static final DeleteDataFormat dataFormat = new DeleteDataFormat();
    public DeleteDataFormatPlugin() {}

    /**
     * Returns the {@link DataFormat} associated with this plugin.
     *
     * @return the data format
     */
    public DataFormat getDataFormat() {
        return dataFormat;
    }
}
