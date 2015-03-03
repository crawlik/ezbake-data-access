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

package ezbake.data.base;

import org.apache.thrift.TException;

import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;

import ezbake.base.thrift.CancelStatus;
import ezbake.base.thrift.EzBakeBaseThriftService;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.data.base.thrift.BaseDataService;
import ezbake.data.base.thrift.PurgeItems;
import ezbake.data.base.thrift.PurgeOptions;
import ezbake.data.base.thrift.PurgeResult;
import ezbake.util.AuditEvent;
import ezbake.util.AuditLogger;

public abstract class EzbakeBaseDataService extends EzBakeBaseThriftService implements BaseDataService.Iface {
    protected static AuditLogger auditLogger;

    private DataServiceMetricSet metrics = null;

    @Override
    public PurgeResult purge(PurgeItems items, PurgeOptions options, EzSecurityToken token) throws TException {
        throw new UnsupportedOperationException("This data service does not support purge!");
    }

    @Override
    public CancelStatus cancelPurge(long purgeId, EzSecurityToken token) throws TException {
        throw new UnsupportedOperationException("This data service does not support purge and/or cancelling purges!");
    }

    public void initAuditLogger(Class<?> clazz) {
        auditLogger = AuditLogger.getAuditLogger(clazz);
    }

    public void logEvent(AuditEvent evt) {
        if (auditLogger != null) {
            auditLogger.logEvent(evt);
        }
    }

    /*
     * Get a MetricSet for common metrics for all data layers.
     */
    public MetricSet getStandardMetrics() {
        return metrics;
    }

    protected void initMetrics(Class<?> cls) {
        metrics = new DataServiceMetricSet(cls);
        getMetricRegistry().registerAll(getStandardMetrics());
    }

    protected Timer.Context startQuery(EzSecurityToken token) {
        return metrics.startQuery();
    }

    protected Timer.Context startGet(EzSecurityToken token) {
        return metrics.startGet();
    }

    protected Timer.Context startPut(EzSecurityToken token) {
        return metrics.startPut();
    }

    protected Timer.Context startUpdate(EzSecurityToken token) {
        return metrics.startUpdate();
    }

    protected Timer.Context startRemove(EzSecurityToken token) {
        return metrics.startRemove();
    }

    protected Timer.Context startPurge(EzSecurityToken token) {
        return metrics.startPurge();
    }
}
