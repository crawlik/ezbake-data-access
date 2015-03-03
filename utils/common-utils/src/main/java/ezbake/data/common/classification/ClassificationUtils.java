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

package ezbake.data.common.classification;

import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.ColumnVisibility.Node;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import ezbake.base.thrift.EzSecurityToken;

public class ClassificationUtils {
    @Deprecated
    public static final String USER_FORMAL_AUTHS = "formalAuths";

    @Deprecated
    public static final String USER_EXTERNAL_COMMUNITY_AUTHS = "externalCommunityAuths";

    private static final Logger logger = LoggerFactory.getLogger(ClassificationUtils.class);

    @Deprecated
    public static boolean confirmAuthsForAccumuloClassification(
            EzSecurityToken security, String accumuloClassification, String authsType) throws VisibilityParseException {
        logger.debug("confirmAuthsForClassification: checking Accumulo classification: " + accumuloClassification);

        return confirmAuthsForClassification(security, accumuloClassification, authsType);
    }

    public static Authorizations getAuthsFromString(final String auths) {
        Authorizations retVal;
        if (auths == null || auths.trim().equals("")) {
            retVal = Authorizations.EMPTY;
        } else {
            retVal = new Authorizations(auths.split(","));
        }
        return retVal;
    }

    public static String extractUserAuths(EzSecurityToken security) {
        final Set<String> l = security.getAuthorizations().getFormalAuthorizations();
        return convertSetToCommaSeparatedString(l);
    }

    public static String extractUserExternalCommunityAuths(EzSecurityToken security) {
        final Set<String> l = security.getAuthorizations().getExternalCommunityAuthorizations();
        return convertSetToCommaSeparatedString(l);
    }

    /**
     * Creates a Classification object from the CAPCO string
     *
     * @return
     */
    public static ezbake.base.thrift.Classification createClassificationFromString(String capco) {
        final ezbake.base.thrift.Classification classification = new ezbake.base.thrift.Classification();
        classification.setCAPCOString(capco);
        return classification;
    }

    /**
     * Helper for getting the Accumulo node's string value
     *
     * @param expression the Accumulo boolean expression
     * @param node the Accumulo node
     */
    public static String getAccumuloNodeTermString(final byte[] expression, final Node node) {
        final int len = node.getTermEnd() - node.getTermStart();
        final byte[] bytes = new ArrayByteSequence(expression, node.getTermStart(), len).toArray();

        return new String(bytes);
    }

    /**
     * Check that the user has valid authorizations to read a document that's for the given Accumulo document
     * classification string.
     * <p/>
     * Useful for determining if a user can insert some data. This method relies on Accumulo's VisibilityEvaluator for
     * boolean expressions.
     *
     * @param security
     * @param booleanExpressionString
     * @param authsType either Formal Visibility auths or External Community auths
     * @return
     * @throws VisibilityParseException
     */
    private static boolean confirmAuthsForClassification(
            EzSecurityToken security, String booleanExpressionString, String authsType)
            throws VisibilityParseException {
        final ColumnVisibility cv = new ColumnVisibility(booleanExpressionString);
        String userAuthsString = null;
        if (authsType == null || authsType.equals(USER_FORMAL_AUTHS)) {
            userAuthsString = extractUserAuths(security);
        } else if (authsType.equals(USER_EXTERNAL_COMMUNITY_AUTHS)) {
            userAuthsString = extractUserExternalCommunityAuths(security);
        }

        logger.debug("confirmAuthsForClassification: userAuthsString: " + userAuthsString);

        final Authorizations userAuths = getAuthsFromString(userAuthsString);
        final VisibilityEvaluator ct = new VisibilityEvaluator(userAuths);

        final boolean authorized = ct.evaluate(cv);

        logger.debug("confirmAuthsForClassification: " + authorized);

        return authorized;
    }

    private static String convertSetToCommaSeparatedString(Set<String> set) {
        return set == null ? "" : Joiner.on(',').join(set);
    }
}
