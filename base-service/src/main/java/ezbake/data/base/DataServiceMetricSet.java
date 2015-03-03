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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.HashMap;

public class DataServiceMetricSet implements MetricSet {

    private Class<?> cls;
    private Map<String, Metric> metrics;
    
    private Timer queryTimer = new Timer();
    private Timer getTimer = new Timer();
    private Timer putTimer = new Timer();
    private Timer purgeTimer = new Timer();
    private Timer updateTimer = new Timer();
    private Timer removeTimer = new Timer();
    
    public DataServiceMetricSet(Class<?> cls) {
        this.cls = cls;
        metrics = new HashMap<>();
        metrics.put("ezbake-query-" + cls.toString(), queryTimer);
        metrics.put("ezbake-get-object-" + cls.toString(), getTimer);
        metrics.put("ezbake-put-object-" + cls.toString(), putTimer);
        metrics.put("ezbake-update-object-" + cls.toString(), updateTimer);
        metrics.put("ezbake-purge-" + cls.toString(), purgeTimer);
        metrics.put("ezbake-remove-" + cls.toString(), removeTimer);
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return metrics;
    }
    
    public Timer.Context startQuery() {
        return queryTimer.time();
    }
    
    public Timer.Context startGet() {
        return getTimer.time();
    }
    
    public Timer.Context startPut() {
        return putTimer.time();
    }
    
    public Timer.Context startUpdate() {
        return updateTimer.time();
    }
    
    public Timer.Context startPurge() {
        return purgeTimer.time();
    }
    
    public Timer.Context startRemove() {
        return removeTimer.time();
    }
}