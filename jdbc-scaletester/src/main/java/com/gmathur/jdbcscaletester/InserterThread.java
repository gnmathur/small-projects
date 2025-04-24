package com.gmathur.jdbcscaletester;

import com.google.common.io.BaseEncoding;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 DATE 	LocalDate
 TIME [ WITHOUT TIME ZONE ] 	LocalTime
 TIMESTAMP [ WITHOUT TIME ZONE ] 	LocalDateTime
 TIMESTAMP WITH TIME ZONE 	OffsetDateTime
 */
public class InserterThread implements Runnable {
    final Connection c;
    final BlockingQueue<BigDecimal> idsCreatedQ;
    int batchSize;
    int id = 0;

    public InserterThread(final Connection c, final BlockingQueue<BigDecimal> idsCreatedQ, final int batchSize) {
        this.c = c;
        this.idsCreatedQ = idsCreatedQ;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        PreparedStatement pst;
        try {
            pst = c.prepareStatement("INSERT INTO film(film_id, title, description, length, created_at) values (?, ?, ?, ?, ?)");
            long idBase = System.currentTimeMillis();
            for (int i = 0; i < batchSize; i++) {
                BigDecimal filmId = BigDecimal.valueOf(idBase+i);
                pst.setBigDecimal(1, filmId);
                pst.setString(2, "We all live in a yellow submarine");
                pst.setString(3, "01234567890123456789012345678901234567890123456789");
                idsCreatedQ.put(filmId);
                id++;
            }
            pst.executeBatch();
            System.out.println("sent batch");
            pst.close();
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}