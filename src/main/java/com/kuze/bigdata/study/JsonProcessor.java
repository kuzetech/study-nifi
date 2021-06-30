/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kuze.bigdata.study;

import com.jayway.jsonpath.JsonPath;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.RedisType;
import org.springframework.data.redis.connection.RedisConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author phillip
 */
@SideEffectFree
@Tags({"JSON", "NIFI ROCKS"})
@CapabilityDescription("Fetch value from json path.")
public class JsonProcessor extends AbstractProcessor {
   
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    public static final String MATCH_ATTR = "match";
    
    public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
            .name("Json Path")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIS_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("redis-connection-pool")
            .displayName("Redis Connection Pool")
            .identifiesControllerService(RedisConnectionPool.class)
            .required(true)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    private volatile RedisConnectionPool redisConnectionPool;

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();

        final RedisConnectionPool redisConnectionPool = validationContext.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPool.class);
        if (redisConnectionPool != null) {
            final RedisType redisType = redisConnectionPool.getRedisType();
            if (redisType != null && redisType == RedisType.CLUSTER) {
                results.add(new ValidationResult.Builder()
                        .subject(REDIS_CONNECTION_POOL.getDisplayName())
                        .valid(false)
                        .explanation(REDIS_CONNECTION_POOL.getDisplayName()
                                + " is configured in clustered mode, and this service requires a non-clustered Redis")
                        .build());
            }
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.redisConnectionPool = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPool.class);
    }

    @OnDisabled
    public void onDisabled() {
        this.redisConnectionPool = null;
    }
    
    @Override
    public void init(final ProcessorInitializationContext context){
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(REDIS_CONNECTION_POOL);
        properties.add(JSON_PATH);
        this.properties = Collections.unmodifiableList(properties);
        
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        RedisConnectionPool service = context.getProperty(REDIS_CONNECTION_POOL).asControllerService(RedisConnectionPool.class);
        RedisConnection connection = service.getConnection();

        final AtomicReference<String> value = new AtomicReference<>();
        
        FlowFile flowfile = session.get();
        
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    String json = IOUtils.toString(in);
                    String result = JsonPath.read(json, context.getProperty(JSON_PATH).getValue());
                    byte[] bytes = connection.get("test".getBytes());
                    if(bytes == null){
                        value.set("my");
                    }else{
                        value.set(new String(bytes));
                    }

                }catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read json string.");
                }finally {
                    connection.close();
                }
            }
        });
        
        // Write the results to an attribute 
        String results = value.get();
        if(results != null && !results.isEmpty()){
            flowfile = session.putAttribute(flowfile, "match", results);
        }
        
        // To write the results back out ot flow file 
        flowfile = session.write(flowfile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(value.get().getBytes());
            }
        });
        
        session.transfer(flowfile, SUCCESS);                                                                                                                                                                                                                                                                                                                                                                                                 
    }
    
    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }
    
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }
    
}
