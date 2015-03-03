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

 package ezbake.data.graph.rexster;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.thrift.TException;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tinkerpop.rexster.filter.AbstractSecurityFilter;
import com.tinkerpop.rexster.protocol.msg.MessageType;
import com.tinkerpop.rexster.protocol.msg.SessionRequestMessage;
import com.tinkerpop.rexster.protocol.serializer.RexProSerializer;
import com.tinkerpop.rexster.protocol.serializer.json.JSONSerializer;
import com.tinkerpop.rexster.protocol.serializer.msgpack.MsgPackSerializer;
import com.tinkerpop.rexster.protocol.server.RexProRequest;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftUtils;

/**
 * SecurityFilter for the Rexster Server. This filter gets the EzSecurityToken via the authenticate method, verifies it
 * with the EzSecurity service, provides the token to Rexster using a ThreadLocal and Rexster passes it along to the
 * Blueprints security wrapper where it can be used.
 */
public class SecurityTokenSecurityFilter extends AbstractSecurityFilter {
    private static final Logger logger = LoggerFactory.getLogger(SecurityTokenSecurityFilter.class);

    /**
     * ThreadLocal responsible for holding the EzBakeSecurityToken associated with the incoming request.
     */
    private static final InheritableThreadLocal<EzSecurityToken> securityToken = new InheritableThreadLocal<>();

    /**
     * EzSecurity client used to verify and get the security token.
     */
    private EzbakeSecurityClient securityClient;

    /**
     * Gets the security token associated with this session.
     *
     * @return The wrapped EzBakeSecurityToken associated with this session.
     */
    public static EzSecurityToken getEzBakeSecurityToken() {
        return securityToken.get();
    }

    /**
     * Takes the first value in the 'Authorization' header and de-serializes it into an EzSecurity token. The
     * securityToken ThreadLocal's value is set with the result of this operation.
     *
     * @param token The Base64 encoded String representation of an EzSecurityToken.
     * @param password Not used.
     * @return Unused - always returns true.
     */
    @Override
    public boolean authenticate(String token, String password) {
        EzSecurityToken ezToken;
        try {
            ezToken = ThriftUtils.deserializeFromBase64(EzSecurityToken.class, token);
        } catch (TException e) {
            final String errMsg = "Error trying to deserialize EzSecurityToken from request authorization header.";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }

        if (securityClient == null) {
            throw new IllegalStateException("EzSecurityClient never configured for SecurityTokenSecurityFilter");
        }

        try {
            securityClient.validateReceivedToken(ezToken);
        } catch (EzSecurityTokenException e) {
            final String errMsg = "Failed to validate security token.";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
        logger.debug(
                "token successfully validated: {}", ezToken);
        securityToken.set(ezToken);
        return true;
    }

    @Override
    public void configure(XMLConfiguration xmlConfiguration) {
        Properties properties = xmlConfiguration.getProperties(XMLConfigurationAdapter.EZBAKE_PROPERTY_KEY);
        if (properties == null) {
            try {
                properties = new EzConfiguration().getProperties();
            } catch (EzConfigurationLoaderException e) {
                logger.error("Failed to load EzConfiguration", e);
                properties = new Properties();
            }
        }

        configure(properties);
    }

    /**
     * Configure filter with EzBake properties.
     *
     * @param properties EzBake properties.
     */
    public void configure(Properties properties) {
        if (securityClient != null) {
            try {
                securityClient.close();
            } catch (IOException e) {
                logger.error("Error closing EzBake security client during reconfiguration", e);
            }
        }

        securityClient = new EzbakeSecurityClient(properties);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    /**
     * Attempt to read a username from a serialized session message.
     *
     * @param message message bytes
     * @param serializer serializer
     * @return username or null if the message could not be deserialized
     */
    private String tryDeserializeUsername(byte[] message, RexProSerializer serializer) {
        try {
            return serializer.deserialize(message, SessionRequestMessage.class).Username;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Pulls the token out of the request before typical behavior resumes.
     */
    @Override
    public NextAction handleRead(final FilterChainContext ctx) throws IOException {
        final RexProRequest realRequest = ctx.getMessage();

        // We only care about session messages, since those are only ones with usernames.
        if (realRequest.getRequestMessageType() == MessageType.SESSION_REQUEST) {
            // For whatever reason, RexProRequest.deserialize() is private and we don't have access to the byte
            // indicating the serialization type. We can't call process() because that will prematurely bind our
            // session without allowing us to validate the security token in the user field of the session message.
            //
            // Instead, do trial deserializations, starting with msgpack, then JSON.

            byte[] message = realRequest.getRequestMessageBytes();
            String token;

            token = tryDeserializeUsername(message, new MsgPackSerializer());
            if (token == null) {
                token = tryDeserializeUsername(message, new JSONSerializer());
            }

            if (token != null) {
                authenticate(token, "password-not-used");
            }
        }

        return super.handleRead(ctx);
    }
}
