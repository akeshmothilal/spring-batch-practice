package com.springbatch.config;

import org.springframework.batch.item.ItemProcessor;
import com.springbatch.entity.Customer;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {

    @Override
    public Customer process(Customer item) throws Exception {
        // Your processing logic here
        return item;
    }
}
