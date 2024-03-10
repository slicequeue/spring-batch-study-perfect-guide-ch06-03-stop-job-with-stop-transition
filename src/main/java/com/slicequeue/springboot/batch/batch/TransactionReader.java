package com.slicequeue.springboot.batch.batch;

import com.slicequeue.springboot.batch.domain.Transaction;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.transform.FieldSet;

public class TransactionReader implements ItemStreamReader<Transaction> {

  private ItemStreamReader<FieldSet> fieldSetReader;
  private int recordCount = 0;
  private int expectedRecordCount = 0;
  private StepExecution stepExecution;

  public TransactionReader(ItemStreamReader<FieldSet> fieldSetReader) {
    // 전략 패턴
    this.fieldSetReader = fieldSetReader;
  }

  @Override
  public Transaction read()
      throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
    return process(fieldSetReader.read());
  }

  private Transaction process(FieldSet fieldSet) {
    Transaction result = null;

    if (fieldSet != null) {
      if (fieldSet.getFieldCount() > 1) {
        // 일반 기록 행 처리
        result = new Transaction();
        result.setAccountNumber(fieldSet.readString(0));
        result.setTimestamp(fieldSet.readDate(1, "yyyy-MM-DD HH:mm:ss"));
        result.setAmount(fieldSet.readDouble(2));

        recordCount++;
      } else {
        // 레코드 갯 수 Footer 처리
        expectedRecordCount = fieldSet.readInt(0);

        if (expectedRecordCount != this.recordCount) {
          this.stepExecution.setTerminateOnly(); // 스프링 배치 종료 지시 플래그 활성화!!!
        }
      }
    }

    return result;
  }

  public void setFieldSetReader(
      ItemStreamReader<FieldSet> fieldSetReader) {
    this.fieldSetReader = fieldSetReader;
  }

  @BeforeStep
  public void beforeStep(StepExecution execution) {
    this.stepExecution = execution;
  }

//  @AfterStep // 최종 판단 부분 스탭 리스너로의 활용 <- 이전 transition 방식, 주석 처리
//  public ExitStatus afterStep(StepExecution execution) {
//    if (recordCount == expectedRecordCount) {
//      return execution.getExitStatus();
//    } else {
//      return ExitStatus.STOPPED;
//    }
//  }

  @Override
  public void open(ExecutionContext executionContext) throws ItemStreamException {
    this.fieldSetReader.open(executionContext);
  }

  @Override
  public void update(ExecutionContext executionContext) throws ItemStreamException {
    this.fieldSetReader.update(executionContext);
  }

  @Override
  public void close() throws ItemStreamException {
    this.fieldSetReader.close();
  }
}
