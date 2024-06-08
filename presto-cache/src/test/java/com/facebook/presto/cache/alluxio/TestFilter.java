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

package com.facebook.presto.cache.alluxio;

import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.wire.FileInfo;
import com.facebook.presto.cache.alluxio.filter.AlluxioCacheFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestFilter
{
    @Test
    public void testCacheFilter() throws IOException
    {
        URIStatus uri1 = new URIStatus(new FileInfo().setPath("/otherdb"));
        URIStatus uri2 = new URIStatus(new FileInfo().setPath("/user/hive/warehouse/test.db/test/0000001"));

        AlluxioConfiguration alluxioCacheConfig = new InstancedConfiguration(new AlluxioProperties(), true);
        AlluxioCacheFilter cacheFilter = new AlluxioCacheFilter(alluxioCacheConfig, "/home/jichen/work/apache/presto/presto-cache/src/test/resources/cache_filter_rule.json");
        assertFalse(cacheFilter.needsCache(uri1));
        assertTrue(cacheFilter.needsCache(uri2));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        ExtendedLocalCacheFileSystem efs = new ExtendedLocalCacheFileSystem(fs);
        efs.initialize(URI.create("hdfs://nameservice/user/hive/warehouse/test.db/test/0000001"), conf);
        assertNotNull(efs);
    }

    @Test
    public void testPath()
    {
        String[] uris = new String[]
                {
                "hdfs:///path/to/file",
                "hdfs://nameservice/path/to/file",
                "hdfs://host:2232/path/to/file",
                };

        for (String p : uris) {
            URI uri = URI.create(p);
            assertTrue(uri.getPath().equals("/path/to/file"));
        }
    }
}
