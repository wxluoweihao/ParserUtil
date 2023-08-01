package modules;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.trino.connector.system.*;
import io.trino.connector.system.jdbc.*;
import io.trino.operator.table.ExcludeColumns;
import io.trino.operator.table.Sequence;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;

public class SystemConnectorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        Multibinder<SystemTable> globalTableBinder = Multibinder.newSetBinder(binder, SystemTable.class);
        globalTableBinder.addBinding().to(NodeSystemTable.class).in(Scopes.SINGLETON);
//        globalTableBinder.addBinding().to(QuerySystemTable.class).in(Scopes.SINGLETON);
//        globalTableBinder.addBinding().to(TaskSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(CatalogSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TableCommentSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(SchemaPropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TablePropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(MaterializedViewSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(MaterializedViewPropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(ColumnPropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(AnalyzePropertiesSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TransactionsSystemTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(RuleStatsSystemTable.class).in(Scopes.SINGLETON);

        globalTableBinder.addBinding().to(AttributeJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(CatalogJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(ColumnJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TypesJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(ProcedureColumnJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(ProcedureJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(PseudoColumnJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(SchemaJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(SuperTableJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(SuperTypeJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TableJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(TableTypeJdbcTable.class).in(Scopes.SINGLETON);
        globalTableBinder.addBinding().to(UdtJdbcTable.class).in(Scopes.SINGLETON);

        Multibinder.newSetBinder(binder, Procedure.class);

//        binder.bind(KillQueryProcedure.class).in(Scopes.SINGLETON);

        binder.bind(GlobalSystemConnector.class).in(Scopes.SINGLETON);

        Multibinder<ConnectorTableFunction> tableFunctions = Multibinder.newSetBinder(binder, ConnectorTableFunction.class);
        tableFunctions.addBinding().toProvider(ExcludeColumns.class).in(Scopes.SINGLETON);
        tableFunctions.addBinding().toProvider(Sequence.class).in(Scopes.SINGLETON);
    }
}

