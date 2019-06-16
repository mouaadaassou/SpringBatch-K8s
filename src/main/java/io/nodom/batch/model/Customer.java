package io.nodom.batch.model;


import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class Customer {

  private Long id;
  private String firstName;
  private String lastName;
  private Date birthDate;
}
