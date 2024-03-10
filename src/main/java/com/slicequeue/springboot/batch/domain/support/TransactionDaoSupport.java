package com.slicequeue.springboot.batch.domain.support;

import com.slicequeue.springboot.batch.domain.Transaction;
import com.slicequeue.springboot.batch.domain.TransactionDao;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;

public class TransactionDaoSupport extends JdbcTemplate implements TransactionDao {

  public TransactionDaoSupport(DataSource dataSource) {
    super(dataSource);
  }

  @Override
  public List<Transaction> getTransactionsByAccountNumber(String accountNumber) {
    return query(
        "select t.id, t.timestamp, t.amount "
            + "from transaction t inner join account_summary a on "
            + "a.id = t.account_summary_id "
            + "where a.account_number = ?",
        new Object[]{ accountNumber },
        (rs, rowNum) -> {
          Transaction transaction = new Transaction();
          transaction.setAmount(rs.getDouble("amount"));
          transaction.setTimestamp(rs.getDate("timestamp"));
          return transaction;
        }
    );
  }
}
