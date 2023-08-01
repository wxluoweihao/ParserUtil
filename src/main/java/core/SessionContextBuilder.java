package core;

import com.google.common.collect.ImmutableMap;
import io.trino.client.ProtocolHeaders;
import io.trino.server.SessionContext;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.session.ResourceEstimates;
import io.trino.transaction.TransactionId;

import java.sql.PreparedStatement;
import java.util.*;

public class SessionContextBuilder {
    public static SessionContext build(TransactionId transactionId, String sql) {
        ProtocolHeaders protocolHeaders = ProtocolHeaders.createProtocolHeaders("trino-parser");
        Set<String> clientTags = new HashSet<String>();
        Set<String> capabilities = new HashSet<String>();
        Set<String> resourcesEstimates = new HashSet<String> ();
        Map<String, String> preparedStatements = new HashMap<>();
        Map<String, Map<String, String>> catalogSessionProperties = new HashMap<>();
        ImmutableMap.Builder<String, String> systemProperties = ImmutableMap.builder();
        preparedStatements.put("sql", sql);
        return new SessionContext(
                protocolHeaders,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Identity.ofUser("trino"),
                new SelectedRole(SelectedRole.Type.ALL, Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                clientTags,
                capabilities,
                new ResourceEstimates(Optional.empty(),Optional.empty(),Optional.empty()),
                systemProperties.buildOrThrow(),
                catalogSessionProperties,
                preparedStatements,
                Optional.of(transactionId),
                false,
                Optional.empty());
    }
}
