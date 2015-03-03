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

package ezbake.data.mongo;

import java.util.List;
import java.util.Set;

/**
 * For clients that want to use ORMs such as Spring Data for Mongo,
 * their POJO model classes should inherit from this class.
 * This class contains the security-related fields for each Mongo document,
 * which get populated by RedactHelper.java.
 */
public abstract class EzMongoBasePojo {

    protected String _ezV;
    protected List _ezFV; // stored in Mongo in double array format - for example: [['S','USA'],['S','AUS']]
    protected List _ezExtV; // also stored in Mongo in double array format
    protected Set<Long> _ezObjRV;
    protected Set<Long> _ezObjDV;
    protected Set<Long> _ezObjWV;
    protected Set<Long> _ezObjMV;
    protected Long _ezId;
    protected Boolean _ezComposite;
    protected Set<Long> _ezPurgeIds;
    protected String _ezAppId;

    public String get_ezV() {
        return _ezV;
    }

    public void set_ezV(String _ezV) {
        this._ezV = _ezV;
    }

    public List get_ezFV() {
        return _ezFV;
    }

    public void set_ezFV(List _ezFV) {
        this._ezFV = _ezFV;
    }

    public List get_ezExtV() {
        return _ezExtV;
    }

    public void set_ezExtV(List _ezExtV) {
        this._ezExtV = _ezExtV;
    }

    public Set<Long> get_ezObjRV() {
        return _ezObjRV;
    }

    public void set_ezObjRV(Set<Long> _ezObjRV) {
        this._ezObjRV = _ezObjRV;
    }

    public Set<Long> get_ezObjDV() {
        return _ezObjDV;
    }

    public void set_ezObjDV(Set<Long> _ezObjDV) {
        this._ezObjDV = _ezObjDV;
    }

    public Set<Long> get_ezObjWV() {
        return _ezObjWV;
    }

    public void set_ezObjWV(Set<Long> _ezObjWV) {
        this._ezObjWV = _ezObjWV;
    }

    public Set<Long> get_ezObjMV() {
        return _ezObjMV;
    }

    public void set_ezObjMV(Set<Long> _ezObjMV) {
        this._ezObjMV = _ezObjMV;
    }

    public Long get_ezId() {
        return _ezId;
    }

    public void set_ezId(Long _ezId) {
        this._ezId = _ezId;
    }

    public Boolean get_ezComposite() {
        return _ezComposite;
    }

    public void set_ezComposite(Boolean _ezComposite) {
        this._ezComposite = _ezComposite;
    }

    public Set<Long> get_ezPurgeIds() {
        return _ezPurgeIds;
    }

    public void set_ezPurgeIds(Set<Long> _ezPurgeIds) {
        this._ezPurgeIds = _ezPurgeIds;
    }

    public String get_ezAppId() {
        return _ezAppId;
    }

    public void set_ezAppId(String _ezAppId) {
        this._ezAppId = _ezAppId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("EzMongoBasePojo{");

        if (_ezV != null) {
            sb.append(" _ezV='" + _ezV + '\'');
        }
        if (_ezFV != null) {
            sb.append(" _ezFV='" + _ezFV + '\'');
        }
        if (_ezExtV != null) {
            sb.append(" _ezExtV='" + _ezExtV + '\'');
        }
        if (_ezObjRV != null) {
            sb.append(" _ezObjRV='" + _ezObjRV + '\'');
        }
        if (_ezObjDV != null) {
            sb.append(" _ezObjDV='" + _ezObjDV + '\'');
        }
        if (_ezObjWV != null) {
            sb.append(" _ezObjWV='" + _ezObjWV + '\'');
        }
        if (_ezObjMV != null) {
            sb.append(" _ezObjMV='" + _ezObjMV + '\'');
        }
        if (_ezId != null) {
            sb.append(" _ezId='" + _ezId + '\'');
        }
        if (_ezComposite != null) {
            sb.append(" _ezComposite='" + _ezComposite + '\'');
        }
        if (_ezPurgeIds != null) {
            sb.append(" _ezPurgeIds='" + _ezPurgeIds + '\'');
        }
        if (_ezAppId != null) {
            sb.append(" _ezAppId='" + _ezAppId + '\'');
        }

        sb.append(" }");
        return sb.toString();
    }
}
