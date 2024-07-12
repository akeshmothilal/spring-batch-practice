package com.springbatch.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.springbatch.entity.Customer;
import com.springbatch.repository.CustomerRepository;

import lombok.AllArgsConstructor;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {

	private JobBuilderFactory jobBuilderFactory;
	private StepBuilderFactory stepBuilderFactory;
	private CustomerRepository customerReposirory;

	@Bean
	public FlatFileItemReader<Customer> itemReader() {
		FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<Customer>();

		itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
		itemReader.setName("customerReader");
		itemReader.setLinesToSkip(1);
		itemReader.setLineMapper(lineMapper());
		return itemReader;

	}

	@Bean
	public LineMapper<Customer> lineMapper() {
		DefaultLineMapper<Customer> linerMapper = new DefaultLineMapper<Customer>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");
		BeanWrapperFieldSetMapper<Customer> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<Customer>();

		beanWrapperFieldSetMapper.setTargetType(Customer.class);
		linerMapper.setLineTokenizer(lineTokenizer);
		linerMapper.setFieldSetMapper(beanWrapperFieldSetMapper);
		return linerMapper;
	}

	@Bean
	public ItemProcessor<Customer, Customer> processor() {
	    return new CustomerProcessor();
	}


	@Bean
	public RepositoryItemWriter<Customer> itemWriter() {
		RepositoryItemWriter<Customer> itemWriter = new RepositoryItemWriter<Customer>();
		itemWriter.setRepository(customerReposirory);
		itemWriter.setMethodName("save");
		return itemWriter;
	}
	
	
	  @Bean
	    public Step step1() {
	        return stepBuilderFactory.get("extract-csv-to-DB").<Customer, Customer>chunk(10)
	                .reader(itemReader())
	                .processor(processor()) 
	                .writer(itemWriter())
	                .taskExecutor(taskExecutor())
	                .build();
	    }

	    @Bean
	    public Job runJob() {
	        return jobBuilderFactory.get("importCustomers")
	                .flow(step1()).end().build();

	    }

	    @Bean
	    public TaskExecutor taskExecutor() {
	        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
	        asyncTaskExecutor.setConcurrencyLimit(10);
	        return asyncTaskExecutor;
	    }

}
