package com.gmathur.jdbcscaletester;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;

public class DeleterThread implements Runnable {
    final Connection c;
    final BlockingQueue<BigDecimal> idsCreatedQ;

    public DeleterThread(final Connection c, final BlockingQueue<BigDecimal> idsCreatedQ) {
        this.c = c;
        this.idsCreatedQ = idsCreatedQ;
    }

    @Override
    public void run() {
        PreparedStatement pst;
        try {
            pst = c.prepareStatement("DELETE FROM film as f where f.film_id = ?");
            while (true) {
                BigDecimal id = idsCreatedQ.take();
                pst.setBigDecimal(1, id);
                pst.executeUpdate();
                pst.close();
            }
        } catch (InterruptedException | SQLException e) {
            e.printStackTrace();
        }
    }
}