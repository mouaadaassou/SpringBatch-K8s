package io.nodom.batch.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.nodom.batch.model.Customer;
import org.springframework.batch.item.file.transform.LineAggregator;

public class CustomerLineAggregator implements LineAggregator<Customer> {

  private ObjectMapper objectMapper;

  public CustomerLineAggregator() {
    objectMapper = new ObjectMapper();
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  @Override
  public String aggregate(Customer customer) {
    try {
      return objectMapper.writeValueAsString(customer);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Unable to serialize the Customer ", e);
    }
  }
}
