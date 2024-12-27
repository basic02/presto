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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.File;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class AlluxioCacheFilter
        extends DefaultCacheFilter
{
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = Logger.get(AlluxioCacheFilter.class);
    private static final String CACHE_NAME = "FILTER_CACHE";
    private static final Cache<String, FilterRuleSet> filter_cache = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.MINUTES) // TTL of 10 minutes
            .build();

    private String cacheConfigFile;

    private static FilterRuleSet loadFromFile(String cacheConfigFile)
    {
        FilterRuleSet filterRuleSet;
        try {
            filterRuleSet = mapper.readValue(new File(cacheConfigFile), FilterRuleSet.class);
            log.info(String.format("cache filter config loaded with %d rules", filterRuleSet.getRules().size()));
        }
        catch (Exception ex) {
            filterRuleSet = null;
            log.warn("failed to load rule, cache will be disabled", ex);
        }
        return filterRuleSet;
    }

    public AlluxioCacheFilter(AlluxioConfiguration conf, String cacheConfigFile)
    {
        super(conf, cacheConfigFile);
        this.cacheConfigFile = cacheConfigFile;
    }

    public FilterRuleSet getRules()
    {
        FilterRuleSet filterRuleSet;
        try {
            filterRuleSet = filter_cache.get(CACHE_NAME, new Callable<FilterRuleSet>() {
                @Override
                public FilterRuleSet call() throws Exception
                {
                    return loadFromFile(AlluxioCacheFilter.this.cacheConfigFile);
                }
            });
        }
        catch (Exception ex) {
            log.warn("failed to get rule, cache will be disabled", ex);
            filterRuleSet = null;
        }
        return filterRuleSet;
    }

    @Override
    public boolean needsCache(URIStatus uriStatus)
    {
        URI uri = URI.create(uriStatus.getPath());
        String path = uri.getPath();
        boolean result = false;
        // check if the given path
        FilterRuleSet filterRuleSet = getRules();
        if ((filterRuleSet != null) && (filterRuleSet.getRules() != null) && (!filterRuleSet.getRules().isEmpty())) {
            result = filterRuleSet.getRules().stream().anyMatch(rule -> path.startsWith(rule.getPathPrefix()));
        }
        if (result) {
            log.info(String.format("%s will be granted to use cache", path));
        }
        else {
            log.warn(String.format("%s will BY-PASS cache", path));
        }
        return result;
    }
}
