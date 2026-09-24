package com.batch.demo.testsupport.slow;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DelegatingDataSource;

/**
 * Same Connection-proxy technique as FaultInjectingDataSource, but sleeps instead of
 * throwing - see SlowStagingInsertTrigger for what's matched.
 */
public class SlowingDataSource extends DelegatingDataSource {

    private final SlowStagingInsertTrigger trigger;

    public SlowingDataSource(DataSource targetDataSource, SlowStagingInsertTrigger trigger) {
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
                    && args[0] instanceof String sql) {
                long delay = trigger.delayMillisFor(sql);
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            try {
                return method.invoke(real, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
        return (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }
}
