package io.nodom.batch.config;


import io.nodom.batch.mapper.CustomerLineAggregator;
import io.nodom.batch.mapper.CustomerRowMapping;
import io.nodom.batch.model.Customer;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
public class JdbcToFlatFileJobConfig {

  @Autowired
  private JobBuilderFactory jobBuilderFactory;

  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Autowired
  private DataSource dataSource;

  @Bean
  public JdbcPagingItemReader<Customer>  jdbcPagingItemReader() {
    JdbcPagingItemReader<Customer> jdbcPagingItemReader = new JdbcPagingItemReader<>();

    jdbcPagingItemReader.setDataSource(dataSource);
    jdbcPagingItemReader.setFetchSize(10);
    jdbcPagingItemReader.setRowMapper(new CustomerRowMapping());

    Map<String, Order> sortKeys = new HashMap<>();
    sortKeys.put("id", Order.ASCENDING);

    MySqlPagingQueryProvider mySqlPagingQueryProvider = new MySqlPagingQueryProvider();
    mySqlPagingQueryProvider.setFromClause("FROM t_customer");
    mySqlPagingQueryProvider.setSelectClause("*");
    mySqlPagingQueryProvider.setSortKeys(sortKeys);

    jdbcPagingItemReader.setQueryProvider(mySqlPagingQueryProvider);
    return jdbcPagingItemReader;
  }

  @Bean
  public FlatFileItemWriter<Customer> flatFileItemWriter() throws Exception {
    FlatFileItemWriter<Customer> flatFileItemReader = new FlatFileItemWriter<>();
    flatFileItemReader.setResource(new FileSystemResource("src/main/resources/customers.json"));
    flatFileItemReader.setLineAggregator(new CustomerLineAggregator());
    flatFileItemReader.afterPropertiesSet();

    return flatFileItemReader;
  }

  @Bean
  public Step step() throws Exception {
    return stepBuilderFactory.get("step1").<Customer, Customer>chunk(10).reader(jdbcPagingItemReader()).writer(flatFileItemWriter()).build();
  }

  @Bean
  public Job job() throws Exception {
    return jobBuilderFactory.get("elt").incrementer(new RunIdIncrementer()).start(step()).build();
  }
}
