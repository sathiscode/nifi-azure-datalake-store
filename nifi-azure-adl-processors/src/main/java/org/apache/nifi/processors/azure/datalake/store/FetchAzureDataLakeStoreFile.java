/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.azure.datalake.store;

import com.microsoft.azure.CloudException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.azure.datalake.store.AzureDataLakeStoreFileSystemServiceInterface;

@Tags({"Azure", "Data Lake Store", "get", "files"})
@CapabilityDescription("Fetches the content of a file from from Azure Data Lake Store and and overwrites the contents of an incoming FlowFile with the content of the Data Lake Store file. If no incoming FlowFile, a new FlowFile is created")
@SeeAlso({ListAzureDataLakeStore.class, PutAzureDataLakeStoreFile.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on Azure Data Lake")})
public class FetchAzureDataLakeStoreFile extends AbstractProcessor {

    public static final PropertyDescriptor ADLS_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Azure Data Lake Store Client Service")
            .description("Specifies the Controller Service to use for accessing Azure Data Lake Store.")
            .required(true)
            .identifiesControllerService(AzureDataLakeStoreFileSystemServiceInterface.class)
            .build();

    public static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("File to Fetch")
            .description("The fully-qualified filename of the file to fetch from Azure Data Lake Store")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${absolute.path}/${filename}")
            .required(true)
            .expressionLanguageSupported(false)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("All FlowFiles that are received are routed to success")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ADLS_CLIENT_SERVICE);
        props.add(FILENAME);
        this.descriptors = Collections.unmodifiableList(props);

        final Set<Relationship> relations = new HashSet<>();
        relations.add(REL_SUCCESS);
        relations.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relations);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ComponentLog logger = this.getLogger();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        logger.debug("FetchAzureDataLakeStoreFile started for " + filename);

        final StopWatch stopWatch = new StopWatch(true);

        try {
            final AzureDataLakeStoreFileSystemServiceInterface adlsService = context.getProperty(ADLS_CLIENT_SERVICE).asControllerService(AzureDataLakeStoreFileSystemServiceInterface.class);
            final InputStream in = adlsService.getFile(filename);

            flowFile = session.importFrom(in, flowFile);

            stopWatch.stop();
            final String dataRate = stopWatch.calculateDataRate(flowFile.getSize());
            final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);

            // emit provenance event and transfer FlowFile
            session.getProvenanceReporter().fetch(flowFile, "", millis);
            session.transfer(flowFile, REL_SUCCESS);

            // it is critical that we commit the session before moving/deleting the remote file. Otherwise, we could have a situation where
            // we ingest the data, delete/move the remote file, and then NiFi dies/is shut down before the session is committed. This would
            // result in data loss! If we commit the session first, we are safe.
            session.commit();

            logger.info("Successfully added {} to flow {} from on Azure Data Lake Store in {} milliseconds at a rate of {}",
                    new Object[]{new Object[]{flowFile}, filename, millis, dataRate});

        } catch (IllegalArgumentException ex) {
            //exception thrown from invalid parameters
            logger.error("Illegal argument exception in FetchAzureDataLakeStoreFile", ex);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        } catch (IOException | CloudException ex) {
            //exception thrown from serialization/deserialization
            logger.error("Exception in FetchAzureDataLakeStoreFile", ex);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            session.getProvenanceReporter().route(flowFile, REL_FAILURE);
        }

    }
}
