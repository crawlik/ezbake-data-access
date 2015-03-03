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

package ezbake.data.common;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.TokenType;
import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.security.client.EzbakeSecurityClient;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class TokenUtils {
    private static final Logger logger = LoggerFactory.getLogger(TokenUtils.class);

    private static String purgeAppSecurityId;

    /**
     * Calls EzbakeSecurityClient's validateReceivedToken method as well as checks to see if the issuedTo securityId ==
     * the app securityId. Note that Common Services should NOT be calling this method; instead call the
     * securityClient's validateReceivedToken method only.
     *
     * @param token  EzSecurityToken
     * @param config Properties
     * @throws TException
     */
    public static void validateSecurityToken(EzSecurityToken token, Properties config) throws TException {
        @SuppressWarnings("resource")
        final EzbakeSecurityClient client = new EzbakeSecurityClient(config);
        try {
            logger.debug(config.toString());
            client.validateReceivedToken(token);
            final String fromId = token.getValidity().getIssuedTo();
            final String appSecId = config.getProperty(EzBakePropertyConstants.EZBAKE_SECURITY_ID);
            if (!fromId.equals(appSecId)) {
                throw new TException("Mismatched Security Id's: " + fromId + " != " + appSecId);
            }
        } catch (final Exception e) {
            e.printStackTrace();
            throw new TException(e);
        } finally {
            try {
                client.close();
            } catch (final IOException e) {
                // ignore
            }
        }
    }

    /**
     * Extracts UserDN from passed in token.
     *
     * @param userToken passed in token
     * @return extracted UserDN
     */
    public static String getUserDN(EzSecurityToken userToken) {
        return userToken.getType() == TokenType.USER ? userToken.getTokenPrincipal().getPrincipal() :
                "N/A - Service Request";
    }

    /**
     * Extracts authorizations from passed in token.
     *
     * @param userToken passed in token.
     * @return comma-delimited string of authorizations.
     */
    public static String getAuths(EzSecurityToken userToken) {
        return userToken.isSetAuthorizations() ? StringUtils.join(
                userToken.getAuthorizations().getFormalAuthorizations(), ",") : "N/A - No auths set";
    }
}
