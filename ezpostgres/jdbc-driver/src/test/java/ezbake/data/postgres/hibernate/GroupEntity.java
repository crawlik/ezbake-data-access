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

package ezbake.data.postgres.hibernate;

import ezbake.base.thrift.Visibility;
import ezbake.thrift.ThriftUtils;
import org.apache.thrift.TException;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import java.util.List;

@Entity
public class GroupEntity {

    @Id
    @Column
    private long groupId;

    @Column(nullable = false, unique = true)
    private String groupName;

    @Column(columnDefinition = "varchar(32672)")
    private String visibility;

    @Column(name = "value")
    private float aFloat;

    @ManyToMany
    private List<UserEntity> users;

    public Visibility getVisibility() throws TException {
        return ThriftUtils.deserializeFromBase64(Visibility.class, this.visibility);
    }

    public void setVisibility(Visibility v) throws TException {
        this.visibility = ThriftUtils.serializeToBase64(v);
    }
}
