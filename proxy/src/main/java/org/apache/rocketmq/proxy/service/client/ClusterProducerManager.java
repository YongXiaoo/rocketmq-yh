package org.apache.rocketmq.proxy.service.client;

import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.common.utils.StartAndShutdown;
import org.apache.rocketmq.proxy.service.sysmessage.HeartbeatSyncer;

public class ClusterProducerManager extends ProducerManager implements StartAndShutdown {

    protected HeartbeatSyncer heartbeatSyncer;

    @Override
    public synchronized void registerProducer(String group, ClientChannelInfo clientChannelInfo) {
        super.registerProducer(group, clientChannelInfo);
    }

    @Override
    public void shutdown() throws Exception {
        this.heartbeatSyncer.shutdown();
    }

    @Override
    public void start() throws Exception {
        this.heartbeatSyncer.start();
    }
}
