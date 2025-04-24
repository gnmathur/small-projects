package com.gmathur.jdbcscaletester;

import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.*;

public class App {
    public static void main(String[] args) throws SQLException {
        System.out.println("Starting");

        JdbcConnection c = new JdbcConnection("jdbc:postgresql://dev137.meraki.com/little_shipper", "meraki_test_helper", "", false);

        BlockingQueue<BigDecimal> idsCreatedQ = new LinkedBlockingDeque<>(5000);

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);
        scheduler.scheduleAtFixedRate(new DeleterThread(c.connection(), idsCreatedQ), 1, 5, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(new InserterThread(c.connection(), idsCreatedQ, 1000), 1, 1, TimeUnit.SECONDS);

        Statement st = c.statement();
        ResultSet rs = st.executeQuery("SELECT * FROM indexed as i WHERE i.id = 56789");
        while (rs.next())
        {
            System.out.println(rs.getString(1) + "| " + rs.getString(2) + "| " + rs.getString(3));
        }
        System.out.println("Done");
        rs.close();
        st.close();
    }
}
