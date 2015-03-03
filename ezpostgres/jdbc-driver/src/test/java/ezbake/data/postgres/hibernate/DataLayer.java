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

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;

import ezbake.data.postgres.hibernate.BasicModel;
import ezbake.thrift.ThriftUtils;
import org.apache.thrift.TException;
import org.hibernate.Session;

import ezbake.data.postgres.hibernate.HibernateUtil.SessionType;

import java.util.Properties;

/**
 * Created by klilly on 10/3/14.
 */
public class DataLayer {

    private Properties ezConfiguration;

    public DataLayer() {
        try {
            EzProperties properties = new EzProperties(new EzConfiguration(new ClasspathConfigurationLoader()).getProperties(), true);
            ezConfiguration = properties;
        } catch (EzConfigurationLoaderException e) {
            ezConfiguration = new Properties();
        }

    }

    public void doSomething(EzSecurityToken token, SessionType sessionType) throws TException {
        System.out.println("Hibernate + Postgres");
        Session session = HibernateUtil.getSessionFactory(ezConfiguration, token, sessionType).openSession();

        Visibility visibility = new Visibility();
        visibility.setFormalVisibility("U");

        BasicModel basicModel = new BasicModel();
        basicModel.setUserId(100);
        basicModel.setUsername("jdoe");
        basicModel.setFirstName("John");
        basicModel.setLastName("Doe");
        basicModel.setVisibility(ThriftUtils.serializeToBase64(visibility));
        
        //delete in case this already exists
        session.beginTransaction();
        session.delete(basicModel);
        session.getTransaction().commit();
        
        //save
        session.beginTransaction();
        session.save(basicModel);
        session.getTransaction().commit();
        
        //delete
        session.beginTransaction();
        session.delete(basicModel);
        session.getTransaction().commit();
    }
}
