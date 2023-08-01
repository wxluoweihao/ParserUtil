package modules;

import com.google.inject.*;
import com.google.inject.Module;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;
import io.trino.server.ForStartup;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class NodeModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.disableCircularProxies();
//        binder.bind(NodeConfig.class).in(Scopes.SINGLETON);
//        binder.bind(NodeInfo.class).in(Scopes.SINGLETON);
//        configBinder(binder).bindConfig(NodeConfig.class);
//        newExporter(binder).export(NodeInfo.class).withGeneratedName();
    }

    @Provides
    @Singleton
    private NodeConfig initNodeConfig() {
        return new NodeConfig()
                .setEnvironment("trinoenv")
                .setNodeId("trinoNode");
    }

    @Provides
    @Singleton
    private NodeInfo initNodeInfo(NodeConfig nodeConfig) {
        return new NodeInfo(nodeConfig);
    }
}