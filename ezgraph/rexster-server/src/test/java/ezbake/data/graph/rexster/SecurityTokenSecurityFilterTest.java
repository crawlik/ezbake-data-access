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

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.concurrent.CyclicBarrier;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.configuration.ClasspathConfigurationLoader;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.data.test.TestUtils;
import ezbake.thrift.ThriftUtils;

public class SecurityTokenSecurityFilterTest {
    /**
     * A specific EzSecurityToken that can be used to validate the output of {@link
     * ezbake.data.graph.rexster.SecurityTokenSecurityFilter#authenticate(String, String)}
     */
    private static final EzSecurityToken USER_WITH_TS_S_B = TestUtils.createTS_S_B_User();

    private Properties properties;

    @Before
    public void setUp() throws EzConfigurationLoaderException {
        properties = new EzConfiguration(new ClasspathConfigurationLoader()).getProperties();
    }

    /**
     * Tests that the first String parameter passed into the authenticate method gets properly de-serialized and put in
     * the EzSecurityToken ThreadLocal.
     *
     * @throws org.apache.thrift.TException If an exception occurs serializing the token.
     */
    @Test
    public void testAuthenticateSetsToken() throws TException {
        final String tokenHeader = ThriftUtils.serializeToBase64(USER_WITH_TS_S_B);
        final SecurityTokenSecurityFilter filter = new SecurityTokenSecurityFilter();
        filter.configure(properties);
        filter.authenticate(tokenHeader, null);

        assertEquals(USER_WITH_TS_S_B, SecurityTokenSecurityFilter.getEzBakeSecurityToken());
    }

    /**
     * Tests the implementation of SecurityTokenSecurityFilter's ThreadLocal holder for an EzSecurityToken using a
     * CyclicBarrier. Each thread/'request' calls authenticate to set the thread local and no thread checks its value
     * until all threads have called authenticate. If each thread shared the same instance stored in the ThreadLocal,
     * then one Thread should have the wrong token when it is checked (last Thread to call authenticate would have the
     * correct token only).
     */
    @Test
    public void testThreadLocalSetAppropriately() {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final SecurityTokenSecurityFilter filter = new SecurityTokenSecurityFilter();
        filter.configure(properties);

        final EzSecurityToken token1 = TestUtils.createTSUser();
        token1.getTokenPrincipal().setName("token1");
        final EzSecurityToken token2 = TestUtils.createTSUser();
        token2.getTokenPrincipal().setName("token2");

        new Thread(new MockConcurrentAuthentication(token1, filter, barrier)).start();
        new Thread(new MockConcurrentAuthentication(token2, filter, barrier)).start();
    }

    /**
     * Calls authenticate on a passed in SecurityTokenSecurityFilter in order to set it's ThreadLocal.  After other
     * threads run in parallel have reached the same step, verifies that it has the correct EzSecurityToken. A
     * CyclicBarrier is used to enforce that all threads call Authenticate before checking their corresponding token.
     */
    private static class MockConcurrentAuthentication implements Runnable {
        /**
         * Set as the value in {@code _filter}'s ThreadLocal
         */
        private final EzSecurityToken _token;

        /**
         * SecurityTokenSecurityFilter on which we are testing.
         */
        private final SecurityTokenSecurityFilter _filter;

        /**
         * CyclicBarrier so we can test concurrency by blocking all threads before checking their values.
         */
        private final CyclicBarrier _barrier;

        /**
         * Constructor.
         *
         * @param token Token to call authenticate with.
         * @param filter Filter to call authenticate on.
         * @param barrier Barrier keeping track of Thread progress.
         */
        MockConcurrentAuthentication(EzSecurityToken token, SecurityTokenSecurityFilter filter, CyclicBarrier barrier) {
            _token = token;
            _barrier = barrier;
            _filter = filter;
        }

        @Override
        public void run() {
            try {
                _filter.authenticate(ThriftUtils.serializeToBase64(_token), "none");
                _barrier.await();
            } catch (final Exception e) {
                throw new RuntimeException("Failed to serialize token or issue with thread.", e);
            }
            assertEquals(_token, SecurityTokenSecurityFilter.getEzBakeSecurityToken());
        }
    }
}
