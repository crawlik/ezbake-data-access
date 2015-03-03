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

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.ColumnVisibility.Node;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.commons.lang.StringUtils;

public class VisibilityUtils {
    /**
     * Given an accumulo-style boolean expression string, return a list of string arrays which
     * represent the permutations of the visibilities
     *
     * @param classification
     * @return
     * @throws VisibilityParseException
     */
    public static List<Object> generateVisibilityList(String classification) throws VisibilityParseException {
        List<Object> classificationList = new ArrayList<>();

        if (!StringUtils.isEmpty(classification)) {
            final ColumnVisibility visibility = new ColumnVisibility(classification);
            evaluateAccumuloExpression(visibility.getExpression(), visibility.getParseTree(), classificationList);

            // We need to check such that they are all wrapped in sublists
            if (classificationList.size() == 1 && classificationList.get(0) instanceof String) {
                final ArrayList check = new ArrayList();
                check.add(classificationList);

                classificationList = check;
            }
        }

        return classificationList;
    }

    /**
     * Based on Accumulo's VisibilityEvaluator.java's "evaluate" method; From the Accumulo-style boolean expression
     * string, generates the security tagging field format for Mongo's $redact operator. The final returned list should
     * be inserted into another list to make the double array format.
     */
    private static void evaluateAccumuloExpression(final byte[] expression, final Node root, List<Object> list)
            throws VisibilityParseException {
        switch (root.getType()) {
            case TERM:
                final String term = ClassificationUtils.getAccumuloNodeTermString(expression, root);
                list.add(term);
                break;
            case AND:
                if (root.getChildren() == null || root.getChildren().size() < 2) {
                    throw new VisibilityParseException(
                            "AND has less than 2 children", expression, root.getTermStart());
                }
                final List<Object> andList = new ArrayList<>();

                for (final Node child : root.getChildren()) {
                    evaluateAccumuloExpression(expression, child, andList);
                }

                // TODO: i shouldn't have to do this; should just be an add
                if (andList.get(0) instanceof List) {
                    list.addAll(andList);
                } else {
                    list.add(andList);
                }

                break;
            case OR:
                if (root.getChildren() == null || root.getChildren().size() < 2) {
                    throw new VisibilityParseException("OR has less than 2 children", expression, root.getTermStart());
                }
                final List<Object> saved = new ArrayList<>();
                saved.addAll(list);
                list.clear();

                for (final Node child : root.getChildren()) {
                    final List<Object> orList = new ArrayList<>();
                    orList.addAll(saved);

                    evaluateAccumuloExpression(expression, child, orList);

                    list.add(orList);
                }

                break;
            // $CASES-OMITTED$
            default:
                throw new VisibilityParseException("No such node type", expression, root.getTermStart());
        }
    }
}
