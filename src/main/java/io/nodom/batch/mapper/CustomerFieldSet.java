package io.nodom.batch.mapper;

import io.nodom.batch.model.Customer;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class CustomerFieldSet implements FieldSetMapper<Customer> {

  @Override
  public Customer mapFieldSet(FieldSet fieldSet) throws BindException {
    return new Customer(fieldSet.readLong("id"), fieldSet.readString("firstName"),
        fieldSet.readString("lastName"), fieldSet.readDate("birthDate"));
  }
}
