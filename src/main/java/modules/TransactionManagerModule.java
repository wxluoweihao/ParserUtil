package modules;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.connector.*;
import io.trino.server.ServerConfig;
import io.trino.transaction.InMemoryTransactionManagerModule;
import io.trino.transaction.NoOpTransactionManager;
import io.trino.transaction.TransactionManager;

public class TransactionManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class).setCoordinator(true);
        if (serverConfig.isCoordinator()) {
            install(new InMemoryTransactionManagerModule());
        }
        else {
            binder.bind(TransactionManager.class).to(NoOpTransactionManager.class).in(Scopes.SINGLETON);
        }
    }
}
