package io.nodom.batch.mapper;

import io.nodom.batch.model.Customer;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.springframework.jdbc.core.RowMapper;

public class CustomerRowMapping implements
    RowMapper<Customer> {

  @Override
  public Customer mapRow(ResultSet resultSet, int i) throws SQLException {
    return new Customer(resultSet.getLong("id"), resultSet.getString("firstName"),
        resultSet.getString("lastName"), resultSet.getDate("birthDate"));
  }
}
