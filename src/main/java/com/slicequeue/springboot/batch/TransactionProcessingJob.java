package com.slicequeue.springboot.batch;

import com.slicequeue.springboot.batch.domain.Transaction;
import javax.sql.DataSource;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
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
        .<Transaction, Transaction>chunk(100) // 1-3 청크 사이즈 지정
        .reader(transactionReader())                    // 1-1 거래 정보를 읽는 리더
        .writer(transactionWriter(null))      // 1-2 jdbc 삽입 롸이터
        .allowStartIfComplete(true)                     // 1-5 잡이 재시작시 다시 실행 될 수 있도록 설정
        .listener(transactionReader())                  // 1-4 리더(1-1) 재활용
        .build();
  }




  public static void main(String[] args) {
    SpringApplication.run(TransactionProcessingJob.class, args);
  }

}
