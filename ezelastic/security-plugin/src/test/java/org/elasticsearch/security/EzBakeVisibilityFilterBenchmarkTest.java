/*   Copyright (C) 2013-2014 Computer Sciences Corporation
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
 * limitations under the License. */

package org.elasticsearch.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static ezbake.thrift.ThriftUtils.deserializeFromBase64;
import static ezbake.thrift.ThriftUtils.serializeToBase64;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.thrift.TException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.ScriptFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.github.tlrx.elasticsearch.test.EsSetup;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import ezbake.base.thrift.AdvancedMarkings;
import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.Permission;
import ezbake.base.thrift.PlatformObjectVisibilities;
import ezbake.base.thrift.Visibility;
import ezbake.data.elastic.security.EzSecurityVisibilityFilter;
import ezbake.security.permissions.PermissionEvaluator;
import ezbake.security.permissions.PermissionUtils;
import ezbake.thrift.serializer.Base64Serializer;
import ezbake.thrift.serializer.CachingSerializer;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class EzBakeVisibilityFilterBenchmarkTest extends AbstractBenchmark {
    private static final int NUM_VISIBIILITY_OBJECTS = 250000;
    private static final int HIGH_NUM_ADV_VARIATIONS = 1000;
    private static final int MEDIUM_NUM_ADV_VARIATIONS = 250;
    private static final int SMALL_NUM_ADV_VARIATIONS = 50;
    private static final List<String> highAdvVariations = new ArrayList<>();
    private static final List<String> mediumAdvVariations = new ArrayList<>();
    private static final List<String> smallAdvVariations = new ArrayList<>();
    private static final List<String> noAdvVariations = new ArrayList<>();

    private static final List<String> stringVisibilities = Lists.newArrayList(
            "A&B&(AUS|CAN|GBR|NZL|USA)", "A&B&USA", "C&(AUS|CAN|GBR|NZL|USA)", "C&USA", "C&D&(AUS|CAN|GBR|NZL|USA)",
            "C&D&USA", "C&D&(AUS|CAN|GBR|NZL|USA)", "E&D&USA", "E&D&F&(AUS|CAN|GBR|NZL|USA)", "E&D&F&USA");

    private static final short cacheSize = 500;

    private static Authorizations auths;

    @BeforeClass
    public static void setUpClass() throws Exception {
        setupVisibility(HIGH_NUM_ADV_VARIATIONS, highAdvVariations);
        setupVisibility(MEDIUM_NUM_ADV_VARIATIONS, mediumAdvVariations);
        setupVisibility(SMALL_NUM_ADV_VARIATIONS, smallAdvVariations);
        setupVisibility(0, noAdvVariations);
        auths = getAuths();

        Thread.sleep(2000); // Wait for docs to be added
    }

    private static void setupVisibility(int numDifferentPovs, List<String> visibilities) throws TException {
        Base64Serializer serializer = new Base64Serializer();
        final Random random = new Random();
        for (int i = 0; i < NUM_VISIBIILITY_OBJECTS; i++) {
            final Visibility vis = new Visibility();
            vis.setFormalVisibility(stringVisibilities.get(random.nextInt(stringVisibilities.size() - 1)));

            if (numDifferentPovs > 0) {
                vis.setAdvancedMarkings(new AdvancedMarkings());
                vis.getAdvancedMarkings().setExternalCommunityVisibility(
                        stringVisibilities.get(
                                random.nextInt(stringVisibilities.size() - 1)));
                vis.getAdvancedMarkings().setPlatformObjectVisibility(
                        createPlatformObjectVisibilities(
                                (long) random.nextInt(
                                        numDifferentPovs / stringVisibilities.size())));
            }
            visibilities.add(serializer.serialize(vis));
        }
    }

    private static PlatformObjectVisibilities createPlatformObjectVisibilities(Long... setLongs) {
        final PlatformObjectVisibilities pov = new PlatformObjectVisibilities();
        pov.setPlatformObjectReadVisibility(Sets.newHashSet(setLongs));
        pov.setPlatformObjectWriteVisibility(Sets.newHashSet(setLongs));
        pov.setPlatformObjectDiscoverVisibility(Sets.newHashSet(setLongs));
        pov.setPlatformObjectManageVisibility(Sets.newHashSet(setLongs));

        return pov;
    }

    @Test
    public void testHighVariationSingleCacheMetrics() throws ExecutionException {
        final LoadingCache<String,Set<Permission>> permissionCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(
            new CacheLoader<String, Set<Permission>>() {
                @Override
                public Set<Permission> load(String visibilityBase64) {
                    Visibility visibility = null;
                    try {
                        if (visibilityBase64 == null) {
                            visibility = new Visibility();
                        } else {
                            visibility = deserializeFromBase64(Visibility.class, visibilityBase64);
                        }
                    } catch (final TException e) {
                        System.out.println("Could not deserialize visibility in document to Visibility POJO: "+e.getMessage());
                        return PermissionUtils.NO_PERMS;
                    }
                    return PermissionUtils.getPermissions(auths, visibility, true, PermissionUtils.ALL_PERMS);
                }
            });
        for (final String v : highAdvVariations) {
            permissionCache.getUnchecked(v);
        }
    }

    @Test
    public void testHighVariationEvaluatorMetrics() throws ExecutionException, InstantiationException, IllegalAccessException, TException {
        final PermissionEvaluator evaluator = new PermissionEvaluator(auths);
        final CachingSerializer<String> serializer = new CachingSerializer<String>(new Base64Serializer());
        for (final String v : highAdvVariations) {
            evaluator.getPermissions(serializer.deserialize(Visibility.class,v));
        }
    }

    @Test
    public void testMediumVariationSingleCacheMetrics() {
        final LoadingCache<String,Set<Permission>> permissionCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(
            new CacheLoader<String, Set<Permission>>() {
                @Override
                public Set<Permission> load(String visibilityBase64) {
                    Visibility visibility = null;
                    try {
                        if (visibilityBase64 == null) {
                            visibility = new Visibility();
                        } else {
                            visibility = deserializeFromBase64(Visibility.class, visibilityBase64);
                        }
                    } catch (final TException e) {
                        System.out.println("Could not deserialize visibility in document to Visibility POJO: "+e.getMessage());
                        return PermissionUtils.NO_PERMS;
                    }
                    return PermissionUtils.getPermissions(auths, visibility, true, PermissionUtils.ALL_PERMS);
                }
            });
        for (final String v : mediumAdvVariations) {
            permissionCache.getUnchecked(v);
        }
    }

    @Test
    public void testMediumVariationEvaluatorMetrics() throws InstantiationException, IllegalAccessException, TException {
        final PermissionEvaluator evaluator = new PermissionEvaluator(auths);
        final CachingSerializer<String> serializer = new CachingSerializer<String>(new Base64Serializer());
        for (final String v : mediumAdvVariations) {
            evaluator.getPermissions(serializer.deserialize(Visibility.class,v));
        }
    }

    @Test
    public void testSmallVariationSingleCacheMetrics() {
        final LoadingCache<String,Set<Permission>> permissionCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(
            new CacheLoader<String, Set<Permission>>() {
                @Override
                public Set<Permission> load(String visibilityBase64) {
                    Visibility visibility = null;
                    try {
                        if (visibilityBase64 == null) {
                            visibility = new Visibility();
                        } else {
                            visibility = deserializeFromBase64(Visibility.class, visibilityBase64);
                        }
                    } catch (final TException e) {
                        System.out.println("Could not deserialize visibility in document to Visibility POJO: "+e.getMessage());
                        return PermissionUtils.NO_PERMS;
                    }
                    return PermissionUtils.getPermissions(auths, visibility, true, PermissionUtils.ALL_PERMS);
                }
            });
        for (final String v : smallAdvVariations) {
            permissionCache.getUnchecked(v);
        }
    }

    @Test
    public void testSmallVariationEvaluatorMetrics() throws TException, InstantiationException, IllegalAccessException {
        final PermissionEvaluator evaluator = new PermissionEvaluator(auths);
        final CachingSerializer<String> serializer = new CachingSerializer<String>(new Base64Serializer());
        for (final String v : smallAdvVariations) {
            evaluator.getPermissions(serializer.deserialize(Visibility.class,v));
        }
    }

    private static Authorizations getAuths() {
        final Authorizations thrift = new Authorizations();
        thrift.setFormalAuthorizations(Sets.newHashSet("USA", "D", "C"));
        thrift.setExternalCommunityAuthorizations(Sets.newHashSet("USA", "D", "C"));
        thrift.setPlatformObjectAuthorizations(Sets.newHashSet(1L, 2L, 3L));
        return thrift;
    }
}
