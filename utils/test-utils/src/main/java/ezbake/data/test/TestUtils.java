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

package ezbake.data.test;

import com.google.common.collect.Sets;
import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.EzSecurityPrincipal;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.TokenType;
import ezbake.base.thrift.ValidityCaveats;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.client.EzBakeSecurityClientConfigurationHelper;
import ezbake.security.client.provider.TokenProvider;

import java.util.Properties;
import java.util.Set;

public class TestUtils {
    public final static String MOCK_APP_SEC_ID = "mockAppSecId";
    public final static String MOCK_TARGET_SEC_ID = "somesid";

    public static EzSecurityToken createTSUser() {
        return TestUtils.createTestToken(MOCK_TARGET_SEC_ID, Sets.newHashSet("TS", "USA"), MOCK_APP_SEC_ID);
    }

    public static EzSecurityToken createTS_S_B_User() {
        return TestUtils.createTestToken(MOCK_TARGET_SEC_ID, Sets.newHashSet("TS", "S", "B", "USA"), MOCK_APP_SEC_ID);
    }

    public static EzSecurityToken createTestToken(String... auths) {
        return TestUtils.createTestToken(MOCK_TARGET_SEC_ID, Sets.newHashSet(auths), MOCK_APP_SEC_ID);
    }

    public static EzSecurityToken createTestToken(String targetId, Set<String> auths, String appSecurityId) {
        final EzSecurityToken token = new EzSecurityToken();
        final ValidityCaveats validity = new ValidityCaveats();
        validity.setIssuedTo(appSecurityId);
        validity.setIssuedFor(targetId);
        validity.setNotAfter(System.currentTimeMillis() + 10000);
        validity.setIssuer("UserUtils");
        validity.setSignature("someTestToken");
        token.setValidity(validity);
        token.setType(TokenType.APP);
        final Authorizations authorizations = new Authorizations();
        authorizations.setFormalAuthorizations(auths);
        token.setAuthorizations(authorizations);
        final EzSecurityPrincipal principal = new EzSecurityPrincipal("principal", validity);
        token.setTokenPrincipal(principal);
        return token;
    }

    public static void addSettingsForMock(Properties config) {
        config.setProperty(EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY, "./ssl/testing/certs");
        config.setProperty(EzBakePropertyConstants.THRIFT_USE_SSL, "false");
        config.setProperty(EzBakeSecurityClientConfigurationHelper.USE_MOCK_KEY, "true"); // force Mock test
        config.setProperty(TokenProvider.CLIENT_MODE, TokenProvider.ClientMode.MOCK.getValue());
        config.setProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID, MOCK_APP_SEC_ID);
    }
}
