package com.slicequeue.springboot.batch.domain;

import java.util.List;

public interface TransactionDao {

  List<Transaction> getTransactionsByAccountNumber(String accountNumber);

}
