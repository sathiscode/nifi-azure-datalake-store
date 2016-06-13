/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.nifi.azure.datalake.store;

import com.microsoft.azure.CloudException;
import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.datalake.store.DataLakeStoreFileSystemManagementClient;
import com.microsoft.azure.management.datalake.store.DataLakeStoreFileSystemManagementClientImpl;
import com.microsoft.azure.management.datalake.store.models.FileStatusProperties;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"Azure", "Data Lake Store", "shared", "connection", "service", "file"})
@CapabilityDescription("A controller service for accessing Azure Data Lake Store File System")

public class AzureDataLakeStoreFileSystemService extends AbstractControllerService implements AzureDataLakeStoreFileSystemServiceInterface {

    private static final Logger LOG = LoggerFactory.getLogger(AzureDataLakeStoreFileSystemService.class);
    private static final List<PropertyDescriptor> serviceProperties;
    private volatile DataLakeStoreFileSystemManagementClient fileSystemClient;
    private volatile String accountName;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ACCOUNT_NAME);
        props.add(TENANT_ID);
        props.add(CLIENT_ID);
        props.add(CLIENT_SECRET);
        serviceProperties = Collections.unmodifiableList(props);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return serviceProperties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        LOG.info("Starting Azure Data Lake File System Client service");

        ApplicationTokenCredentials creds = new ApplicationTokenCredentials(
                context.getProperty(CLIENT_ID).getValue(),
                context.getProperty(TENANT_ID).getValue(),
                context.getProperty(CLIENT_SECRET).getValue(), null);

        this.fileSystemClient = new DataLakeStoreFileSystemManagementClientImpl(creds);
        this.accountName = context.getProperty(ACCOUNT_NAME).getValue();
    }

    @OnDisabled
    public void shutdown() {
        LOG.info("Stopping Azure Data Lake File System Client service");
        this.fileSystemClient = null;
    }

    @Override
    public List<FileStatusProperties> listFileStatus(String directoryPath) throws CloudException, IOException, IllegalArgumentException {
        return this.fileSystemClient.getFileSystemOperations().listFileStatus(this.accountName, directoryPath).getBody().getFileStatuses().getFileStatus();
    }

    @Override
    public void createFile(String path) throws IOException, CloudException {
        this.fileSystemClient.getFileSystemOperations().create(this.accountName, path);
    }

    // Create file with contents
    @Override
    public void createFile(String path, byte[] contents, boolean force) throws IOException, CloudException {
        this.fileSystemClient.getFileSystemOperations().create(this.accountName, path, contents, force);
    }

    // Append to file
    @Override
    public void appendToFile(String path, byte[] contents) throws IOException, CloudException {
        this.fileSystemClient.getFileSystemOperations().append(this.accountName, path, contents);
    }

    @Override
    public InputStream getFile(String path) throws IOException, CloudException {
        return this.fileSystemClient.getFileSystemOperations().open(this.accountName, path).getBody();
    }

    // Concatenate files
    @Override
    public void concatenateFiles(List<String> srcFilePaths, String destFilePath) throws IOException, CloudException {
        this.fileSystemClient.getFileSystemOperations().concat(this.accountName, destFilePath, srcFilePaths);
    }

    // Delete concatenated file
    @Override
    public void deleteFile(String filePath) throws IOException, CloudException {
        this.fileSystemClient.getFileSystemOperations().delete(this.accountName, filePath);
    }
}
