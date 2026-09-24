package com.batch.demo.testsupport.fault;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DelegatingDataSource;

/**
 * Wraps the real DataSource - including JobRepository's own JDBC access, since this
 * app has exactly one DataSource bean - but is scoped to a no-op for everything except
 * a matching "insert into orders" prepareStatement call, so JobRepository's BATCH_*
 * bookkeeping is never affected. See OrderInsertFaultTrigger for the matching/arming
 * logic.
 */
public class FaultInjectingDataSource extends DelegatingDataSource {

    private final OrderInsertFaultTrigger trigger;

    public FaultInjectingDataSource(DataSource targetDataSource, OrderInsertFaultTrigger trigger) {
        super(targetDataSource);
        this.trigger = trigger;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return wrap(super.getConnection());
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return wrap(super.getConnection(username, password));
    }

    private Connection wrap(Connection real) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("prepareStatement".equals(method.getName()) && args != null && args.length > 0
                    && args[0] instanceof String sql && trigger.shouldFailFor(sql)) {
                throw new SQLException("Injected fault: simulated failure inserting into orders");
            }
            try {
                return method.invoke(real, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }
}
