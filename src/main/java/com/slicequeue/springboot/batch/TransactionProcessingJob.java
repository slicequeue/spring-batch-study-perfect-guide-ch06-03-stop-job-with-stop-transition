package com.slicequeue.springboot.batch;

import com.slicequeue.springboot.batch.domain.AccountSummary;
import com.slicequeue.springboot.batch.domain.Transaction;
import com.slicequeue.springboot.batch.domain.TransactionDao;
import com.slicequeue.springboot.batch.domain.support.TransactionDaoSupport;
import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.slicequeue.springboot.batch.batch.*;
import org.springframework.core.io.Resource;

@EnableBatchProcessing
@SpringBootApplication
public class TransactionProcessingJob {

  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Bean // 1-1, 1-4
  @StepScope
  public TransactionReader transactionReader() {
    return new TransactionReader(fileItemReader(null));
  }

  @Bean
  @StepScope
  public FlatFileItemReader<FieldSet> fileItemReader(
      @Value("#{jobParameters['transactionFile']}") Resource inputFile
  ) {
    return new FlatFileItemReaderBuilder<FieldSet>()
        .name("fileItemReader")
        .resource(inputFile)
        .lineTokenizer(new DelimitedLineTokenizer())
        .fieldSetMapper(new PassThroughFieldSetMapper())
        .build();
  }

  @Bean // 1-2
  public JdbcBatchItemWriter<Transaction> transactionWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<Transaction>()
        .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
        .sql("INSERT INTO TRANSACTION "
            + "(ACCOUNT_SUMMARY_ID, TIMESTAMP, AMOUNT) "
            + "VALUES ((SELECT ID FROM ACCOUNT_SUMMARY "
            + "WHERE ACCOUNT_NUMBER = :accountNumber), :timestamp, :amount)")
        .dataSource(dataSource)
        .build();
  }

  @Bean // step1 - 파일의 데이터를 데이터베이스로 옮기는 작업
  public Step importTransactionFileStep() {
    return this.stepBuilderFactory.get("importTransactionFileStep")
        .<Transaction, Transaction>chunk(100) // 1-3 청크 사이즈 100 지정
        .reader(transactionReader())                    // 1-1 거래 정보를 읽는 리더
        .writer(transactionWriter(null))      // 1-2 jdbc 삽입 롸이터
        .allowStartIfComplete(true)                     // 1-5 잡이 재시작시 다시 실행 될 수 있도록 설정
        .listener(transactionReader())                  // 1-4 리더(1-1) 재활용
        .build();
  }


  @Bean //  2-1
  @StepScope
  public JdbcCursorItemReader<AccountSummary> accountSummaryReader(DataSource dataSource) {
   return new JdbcCursorItemReaderBuilder<AccountSummary>()
       .name("accountSummaryReader")
       .dataSource(dataSource)
       .sql("SELECT ACCOUNT_NUMBER, CURRENT_BALANCE, "
           + "FROM ACCOUNT_SUMMARY A "
           + "WHERE A.ID IN ("
           + "  SELECT DISTINCT T.ACCOUNT_SUMMARY_ID "
           + "  FROM TRANSACTION T) "
           + "ORDER BY A.ACCOUNT_NUMBER")
       .rowMapper((resultSet, rowNumber) -> {
         AccountSummary summary = new AccountSummary();
         summary.setAccountNumber(resultSet.getString("ACCOUNT_NUMBER"));
         summary.setCurrentBalance(resultSet.getDouble("CURRENT_BALANCE"));
         return summary;
       })
       .build();
  }

  @Bean
  public TransactionDao transactionDao(DataSource dataSource) {
    return new TransactionDaoSupport(dataSource);
  }

  @Bean // 2-2
  public TransactionApplierProcessor transactionApplierProcessor() {
    return new TransactionApplierProcessor(transactionDao(null));
  }

  @Bean // 2-3
  public JdbcBatchItemWriter<AccountSummary> accountSummaryWriter(DataSource dataSource) {
    return new JdbcBatchItemWriterBuilder<AccountSummary>()
        .dataSource(dataSource)
        .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
        .sql("UPDATE ACCOUNT_SUMMARY "
            + "SET CURRENT_BALANCE = :currentBalance "
            + "WHERE ACCOUNT_NUMBER = :accountNumber")
        .build();
  }

  @Bean //  step2 - 파일에서 찾은 거래 정보를 계좌에 적용하는 처리
  public Step applyTransactionStep() {
    return this.stepBuilderFactory.get("applyTransactionStep")
        .<AccountSummary, AccountSummary>chunk(100)   // 2-4 청크 사이즈 100 지정
        .reader(accountSummaryReader(null))          // 2-1 JdbcCursorItemReader 이용하여 AccountSummary 레코드를 읽는 리더
        .processor(transactionApplierProcessor())              // 2-2 읽은 AccountSummary 레코드 기준으로 각 계좌에 거래정보를 적용하는 프로세서
        .writer(accountSummaryWriter(null))          // 2-3 JdbcBatchItemWriter 를 사용해 갱신된 계좌 요약 레코드를 DB에 저장
        .build();
  }

  @Bean // 3-2
  @StepScope
  public FlatFileItemWriter<AccountSummary> accountSummaryFileWriter(
      @Value("#{jobParameters['summaryFile']}") Resource summaryFile) {

    DelimitedLineAggregator<AccountSummary> lineAggregator = new DelimitedLineAggregator<>();
    BeanWrapperFieldExtractor<AccountSummary> fieldExtractor = new BeanWrapperFieldExtractor<>();
    fieldExtractor.setNames(new String[]{"accountNumber", "currentBalance"});
    fieldExtractor.afterPropertiesSet();
    lineAggregator.setFieldExtractor(fieldExtractor);

    return new FlatFileItemWriterBuilder<AccountSummary>()
        .name("accountSummaryFileWriter")
        .resource(summaryFile)
        .lineAggregator(lineAggregator)
        .build();
  }

  @Bean // step3 - 각 계좌 번호의 현재 잔액 기준으로 CSV 파일을 생성 처리
  public Step generateAccountSummaryStep() {
    return this.stepBuilderFactory.get("generateAccountSummaryStep")
        .<AccountSummary, AccountSummary>chunk(100) // 3-3
        .reader(accountSummaryReader(null)) // 3-1 = 2-1
        .writer(accountSummaryFileWriter(null)) // 3-2 각 레코드의 계좌번호와 현재 잔액으로 CSV 파일 생성
        .build();
  }

  public static void main(String[] args) {
    SpringApplication.run(TransactionProcessingJob.class, args);
  }

}
