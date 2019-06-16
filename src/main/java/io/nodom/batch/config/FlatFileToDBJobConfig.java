package io.nodom.batch.config;


import io.nodom.batch.mapper.CustomerFieldSet;
import io.nodom.batch.model.Customer;
import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
public class FlatFileToDBJobConfig {

  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Autowired
  private DataSource dataSource;

  @Bean
  public FlatFileItemReader<Customer> flatFileItemreader() {
    FlatFileItemReader<Customer> flatFileItemWriter = new FlatFileItemReader<>();
    flatFileItemWriter.setResource(new ClassPathResource("/data/customers.csv"));
    flatFileItemWriter.setLinesToSkip(1);

    DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

    DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
    tokenizer.setNames(new String[]{"id", "firstName", "lastName", "birthDate"});

    lineMapper.setLineTokenizer(tokenizer);
    lineMapper.setFieldSetMapper(new CustomerFieldSet());
    lineMapper.afterPropertiesSet();

    flatFileItemWriter.setLineMapper(lineMapper);
    return flatFileItemWriter;
  }

  @Bean
  public JdbcBatchItemWriter<Customer> jdbcBatchItemWriter() {
    JdbcBatchItemWriter<Customer> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();

    jdbcBatchItemWriter.setDataSource(dataSource);
    jdbcBatchItemWriter
        .setSql("INSERT INTO t_customer VALUES(:id, :firstName, :lastName, :birthDate)");
    jdbcBatchItemWriter
        .setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
    jdbcBatchItemWriter.afterPropertiesSet();

    return jdbcBatchItemWriter;
  }

  @Bean
  public Step stepWithFlatReaderJdbcWriter() {
    return stepBuilderFactory.get("step-flat-jdbc")
        .<Customer, Customer>chunk(10)
        .reader(flatFileItemreader())
        .writer(jdbcBatchItemWriter())
        .build();
  }

  @Bean
  public Job jobWithFlatReaderJdbcWriter() {
    return jobBuilderFactory.get("job-flat-jdbc-101")
        .start(stepWithFlatReaderJdbcWriter())
        .build();
  }
}
