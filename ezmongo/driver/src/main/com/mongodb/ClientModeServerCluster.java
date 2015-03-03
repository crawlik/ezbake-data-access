package com.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static java.lang.String.format;
import static org.bson.util.Assertions.isTrue;

/**
 * Created by jagius on 6/9/14.
 */
public class ClientModeServerCluster extends BaseCluster {
    private final ClientModeServer server;

    private static final Logger LOGGER = Loggers.getLogger("cluster");
    public ClientModeServerCluster(String clusterId, ClusterSettings settings,
                                   ClusterableServerFactory serverFactory, ClusterListener clusterListener) {
        super(clusterId, settings, serverFactory, clusterListener);
        isTrue("one server in a direct cluster", settings.getHosts().size() == 1);
        isTrue("connection mode is ClientMode", settings.getMode() == ClusterConnectionMode.ClientMode);

        LOGGER.info(format("Cluster created with settings %s", settings.getShortDescription()));
        server = new ClientModeServer();
    }

    private void publishDescription(final ServerDescription serverDescription) {
        ClusterType clusterType = getSettings().getRequiredClusterType();
        if (clusterType == ClusterType.Unknown && serverDescription != null) {
            clusterType = serverDescription.getClusterType();
        }
        ClusterDescription description = new ClusterDescription(ClusterConnectionMode.ClientMode, clusterType,
                serverDescription == null ? Collections.<ServerDescription>emptyList() : Arrays.asList(serverDescription));

        updateDescription(description);
        fireChangeEvent();
    }

    /**
     * Return the server at the given address.
     *
     * @param serverAddress the address
     * @return the server, or null if the cluster no longer contains a server at this address.
     */
    @Override
    protected ClusterableServer getServer(ServerAddress serverAddress) {
        return server;
    }

    @Override
    public Server getServer(ServerSelector serverSelector, long maxWaitTime, TimeUnit timeUnit) {
        return server;
    }

    @Override
    public ClusterDescription getDescription(long maxWaitTime, TimeUnit timeUnit) {
        return new ClusterDescription(ClusterConnectionMode.ClientMode,ClusterType.ClientMode,new ArrayList<ServerDescription>());
    }

    @Override
    public ClusterSettings getSettings() {
        return super.getSettings();
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public boolean isClosed() {
        return super.isClosed();
    }

    @Override
    protected synchronized void updateDescription(ClusterDescription newDescription) {
        super.updateDescription(newDescription);
    }

    @Override
    protected void fireChangeEvent() {
        super.fireChangeEvent();
    }

    @Override
    protected Random getRandom() {
        return super.getRandom();
    }

    @Override
    protected ClusterableServer createServer(ServerAddress serverAddress, ChangeListener<ServerDescription> serverStateListener) {
        return server;
    }
}
