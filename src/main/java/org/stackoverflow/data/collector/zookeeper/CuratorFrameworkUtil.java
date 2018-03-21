package org.stackoverflow.data.collector.zookeeper;

import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.stackoverflow.data.collector.utils.CollectorConfigUtil;

public class CuratorFrameworkUtil {
	
	public static CuratorFramework getClient() {
		return CuratorFrameworkFactory.newClient(CollectorConfigUtil.getProperties().getProperty("collector.zookeeper.host"), new ExponentialBackoffRetry(1000, 3));
	}

}
