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

package ezbake.data.postgres;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.security.common.core.EzSecurityClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;

/**
 * A token provider that provides a given, static security token. The token is validated by the security client before
 * being returned so is refreshed if possible and necessary.
 */
class ExplicitTokenProvider implements Provider<EzSecurityToken> {
    private static final Logger logger = LoggerFactory.getLogger(ExplicitTokenProvider.class);
    private EzSecurityClient securityClient;
    private EzSecurityToken explicitToken;

    /**
     * Constructs a new provider that returns the given security token
     *
     * @param securityClient security client
     * @param explicitToken token to provide
     */
    public ExplicitTokenProvider(EzSecurityClient securityClient, EzSecurityToken explicitToken) {
        this.securityClient = securityClient;
        this.explicitToken = explicitToken;
    }

    @Override
    public EzSecurityToken get() {
        EzSecurityToken token = explicitToken;
        try {
            securityClient.validateReceivedToken(token);
        } catch (EzSecurityTokenException e) {
            logger.error("Couldn't validate explicit token in security client", e);
        }

        return token;
    }
}
