package modules;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.connector.*;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNodeManager;

import static io.airlift.tracing.Tracing.noopTracer;

public class CatalogManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(DefaultCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(LazyCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(CatalogFactory.class).to(LazyCatalogFactory.class).in(Scopes.SINGLETON);

        CatalogManagerConfig config = buildConfigObject(CatalogManagerConfig.class)
                .setCatalogMangerKind(CatalogManagerConfig.CatalogMangerKind.DYNAMIC);
        install(new DynamicCatalogManagerModule());
        install(new CatalogServiceProviderModule());
    }

    @Provides
    @Singleton
    private InternalNodeManager initInterNodeManager() {
        return new InMemoryNodeManager();
    }

    @Provides
    @Singleton
    private NodeSchedulerConfig initNodeSchedulerConfig() {
        return new NodeSchedulerConfig();
    }

    @Provides
    @Singleton
    private Tracer initTracer() {
        return noopTracer();
    }

    @Provides
    @Singleton
    private OpenTelemetry initOpenTelemetry() {
        return OpenTelemetry.noop();
    }
}
