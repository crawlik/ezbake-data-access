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

package ezbake.data.graph.blueprints.visibility;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.thrift.TException;

import ezbake.base.thrift.Visibility;
import ezbake.thrift.ThriftUtils;

public class PropertyValueMap implements Map<String, Object> {

    private final Object value;
    private final String visibility;
    private final Boolean delete;

    private static final Set<String> keySet2;
    private static final Set<String> keySet3;

    static {
        final Set<String> ks2 = new HashSet<>(2);
        ks2.add(PropertyFilter.VALUE_KEY);
        ks2.add(PropertyFilter.VISIBILITY_KEY);
        keySet2 = Collections.unmodifiableSet(ks2);

        final Set<String> ks3 = new HashSet<>(3);
        ks3.add(PropertyFilter.VALUE_KEY);
        ks3.add(PropertyFilter.VISIBILITY_KEY);
        ks3.add(PropertyFilter.DELETE_KEY);
        keySet3 = Collections.unmodifiableSet(ks3);
    }

    public PropertyValueMap(Object value, Visibility visibility) {
        this(value, visibility, null);
    }

    public PropertyValueMap(Object value, Visibility visibility, Boolean delete) {
        this.value = value;
        this.delete = delete;

        try {
            this.visibility = ThriftUtils.serializeToBase64(visibility);
        } catch (TException e) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public int size() {
        return delete == null ? 2 : 3;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object o) {
        if (!(o instanceof String)) {
            return false;
        }

        String s = (String) o;

        return (s.equals(PropertyFilter.VALUE_KEY) ||
                s.equals(PropertyFilter.VISIBILITY_KEY) ||
                (delete != null && s.equals(PropertyFilter.DELETE_KEY)));
    }

    @Override
    public boolean containsValue(Object o) {
        return false;
    }

    @Override
    public Object get(Object o) {
        if (!(o instanceof String)) {
            return null;
        }

        String s = (String) o;

        if (s.equals(PropertyFilter.VALUE_KEY)) {
            return value;
        }

        if (s.equals(PropertyFilter.VISIBILITY_KEY)) {
            return visibility;
        }

        if (s.equals(PropertyFilter.DELETE_KEY)) {
            return delete;
        }

        return null;
    }

    @Override
    public Object put(String s, Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ?> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        return delete == null ? keySet2 : keySet3;
    }

    @Override
    public Collection<Object> values() {
        Set<Object> values = new HashSet<>(3);
        values.add(value);
        values.add(visibility);
        if (delete != null) {
            values.add(delete);
        }

        return Collections.unmodifiableCollection(values);
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        Set<Entry<String, Object>> entries = new HashSet<>(3);
        entries.add(new MapEntry<>(PropertyFilter.VALUE_KEY, value));
        entries.add(new MapEntry<String, Object>(PropertyFilter.VISIBILITY_KEY, visibility));
        if (delete != null) {
            entries.add(new MapEntry<String, Object>(PropertyFilter.DELETE_KEY, delete));
        }

        return Collections.unmodifiableSet(entries);
    }

    private static final class MapEntry<String, Object> implements Entry<String, Object> {

        private final String key;
        private final Object value;

        public MapEntry(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public Object setValue(java.lang.Object object) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (!(o instanceof PropertyValueMap)) {
            return false;
        }

        PropertyValueMap rhs = (PropertyValueMap) o;

        return new EqualsBuilder()
                .append(value, rhs.value)
                .append(visibility, rhs.visibility)
                .append(delete, rhs.delete)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(value)
                .append(visibility)
                .append(delete)
                .toHashCode();
    }
}
