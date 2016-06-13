/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.nifi.azure.datalake.store;

import com.microsoft.azure.CloudException;
import com.microsoft.azure.management.datalake.store.models.FileStatusProperties;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"Azure", "Data Lake Store", "shared", "connection", "service", "file"})
@CapabilityDescription("A controller service for accessing Azure Data Lake Store File System")

public interface AzureDataLakeStoreFileSystemServiceInterface extends ControllerService {

    static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("Account Name")
            .description("Azure DataLake Store account name")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor TENANT_ID = new PropertyDescriptor.Builder()
            .name("Tenant ID")
            .description("Tenant ID")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Client ID")
            .description("Client ID")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor CLIENT_SECRET = new PropertyDescriptor.Builder()
            .name("Client Secret")
            .description("Client Secret Key")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    List<FileStatusProperties> listFileStatus(String directoryPath) throws CloudException, IOException, IllegalArgumentException;

    void createFile(String path) throws IOException, CloudException;

    void createFile(String path, byte[] contents, boolean force) throws IOException, CloudException;

    void appendToFile(String path, byte[] contents) throws IOException, CloudException;

    InputStream getFile(String path) throws IOException, CloudException;

    void concatenateFiles(List<String> srcFilePaths, String destFilePath) throws IOException, CloudException;

    void deleteFile(String filePath) throws IOException, CloudException;
}
