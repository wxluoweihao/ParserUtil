package modules;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.connector.*;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.CatalogManager;
import io.trino.server.ServerConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class DynamicCatalogManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(CoordinatorDynamicCatalogManager.class).in(Scopes.SINGLETON);
        CatalogStoreConfig config = buildConfigObject(CatalogStoreConfig.class);
        binder.bind(CatalogStore.class).to(InMemoryCatalogStore.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorServicesProvider.class).to(CoordinatorDynamicCatalogManager.class).in(Scopes.SINGLETON);
        binder.bind(CatalogManager.class).to(CoordinatorDynamicCatalogManager.class).in(Scopes.SINGLETON);
        binder.bind(CoordinatorLazyRegister.class).asEagerSingleton();

//            configBinder(binder).bindConfig(CatalogPruneTaskConfig.class);
//            binder.bind(CatalogPruneTask.class).in(Scopes.SINGLETON);
    }

    private static class CoordinatorLazyRegister
    {
        @Inject
        public CoordinatorLazyRegister(
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory,
                CoordinatorDynamicCatalogManager catalogManager,
                GlobalSystemConnector globalSystemConnector)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }

    private static class WorkerLazyRegister
    {
        @Inject
        public WorkerLazyRegister(
                DefaultCatalogFactory defaultCatalogFactory,
                LazyCatalogFactory lazyCatalogFactory,
                WorkerDynamicCatalogManager catalogManager,
                GlobalSystemConnector globalSystemConnector)
        {
            lazyCatalogFactory.setCatalogFactory(defaultCatalogFactory);
            catalogManager.registerGlobalSystemConnector(globalSystemConnector);
        }
    }
}
