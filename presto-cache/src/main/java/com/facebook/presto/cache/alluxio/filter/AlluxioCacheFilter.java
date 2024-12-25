/*
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

package com.facebook.presto.cache.alluxio.filter;

import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.filter.DefaultCacheFilter;
import alluxio.conf.AlluxioConfiguration;
import alluxio.shaded.client.com.fasterxml.jackson.databind.ObjectMapper;
import com.facebook.airlift.log.Logger;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;

public class AlluxioCacheFilter
        extends DefaultCacheFilter
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = Logger.get(AlluxioCacheFilter.class);

    private FilterRuleSet filterRuleSet;

    public AlluxioCacheFilter(AlluxioConfiguration conf, String cacheConfigFile)
    {
        super(conf, cacheConfigFile);
        try {
            filterRuleSet = mapper.readValue(new File(cacheConfigFile), FilterRuleSet.class);
            log.info(String.format("cache filter initialized with %d rules", filterRuleSet.getRules().size()));
        }
        catch (Exception ex) {
            filterRuleSet = null;
            log.warn("failed to load rule, cache will be disabled", ex);
        }
        log.info("cache filter initialized");
    }

    @Override
    public boolean needsCache(URIStatus uriStatus)
    {
        URI uri = URI.create(uriStatus.getPath());
        String path = uri.getPath();
        boolean result = false;
        // check if the given path
        if ((filterRuleSet != null) && (filterRuleSet.getRules() != null) && (!filterRuleSet.getRules().isEmpty())) {
            result = filterRuleSet.getRules().stream().anyMatch(rule -> path.startsWith(rule.getPathPrefix()));
        }
        if (result) {
            log.info(String.format("%s is cached", path));
        }
        else {
            log.warn(String.format("%s is read by-pass", path));
        }
        return result;
    }
}
