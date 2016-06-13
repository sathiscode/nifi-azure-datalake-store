/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.nifi.processors.azure.datalake.store;

import com.microsoft.azure.CloudException;
import com.microsoft.azure.management.datalake.store.models.FileStatusProperties;
import com.microsoft.azure.management.datalake.store.models.FileType;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.azure.datalake.store.model.FileStatusInfo;
import org.apache.nifi.processors.standard.AbstractListProcessor;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.azure.datalake.store.AzureDataLakeStoreFileSystemServiceInterface;

@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"list", "azure", "data lake", "ingest", "source", "input", "files", "store"})
@CapabilityDescription("Performs a listing of the files residing on an Azure Data Lake Store. For each file that is found, a new FlowFile will be created with the filename attribute "
        + "set to the name of the file on the Azure Data Lake Store. This can then be used in conjunction with FetchDataLakeStore/GetDataLakeStore in order to fetch/get those files.")
@SeeAlso({FetchAzureDataLakeStoreFile.class, PutAzureDataLakeStoreFile.class})
@WritesAttributes({
    @WritesAttribute(attribute = "datalake.store.file.blocksize", description = "The block size of the file"),
    @WritesAttribute(attribute = "datalake.store.file.length", description = "The length of the file"),
    @WritesAttribute(attribute = "datalake.store.file.name", description = "The username of the user that performed the SFTP Listing"),
    @WritesAttribute(attribute = "file.owner", description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = "file.group", description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = "file.type", description = "FILE / DIRECTORY"),})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
        + "This allows the Processor to list only files that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if "
        + "a new Primary Node is selected, the new node will not duplicate the data that was listed by the previous Primary Node.")
public class ListAzureDataLakeStore extends AbstractListProcessor<FileStatusInfo> {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    protected static final PropertyDescriptor ADLS_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Azure Data Lake Store File System Service")
            .description("Specifies the Controller Service to use for accessing Azure Data Lake Store.")
            .required(true)
            .identifiesControllerService(AzureDataLakeStoreFileSystemServiceInterface.class)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Input Directory")
            .description("The input directory from which to list files")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor RECURSE = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether to list files from subdirectories of the directory")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    private final AtomicReference<Pattern> fileFilterRef = new AtomicReference<>();

    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_BLOCK_SIZE_ATTRIBUTE = "file.blockSize";
    public static final String FILE_LENGTH_ATTRIBUTE = "file.length";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> prop = new ArrayList<>();
        prop.add(ADLS_CLIENT_SERVICE);
        prop.add(DIRECTORY);
        prop.add(RECURSE);
        prop.add(FILE_FILTER);
        this.properties = Collections.unmodifiableList(prop);

        final Set<Relationship> rel = new HashSet<>();
        rel.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(rel);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final Pattern filePattern = Pattern.compile(context.getProperty(FILE_FILTER).getValue());
        fileFilterRef.set(filePattern);
    }

    @Override
    protected Map<String, String> createAttributes(final FileStatusInfo fileStatusInfo, final ProcessContext context) {

        final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);

        final Map<String, String> attributes = new HashMap<>();

        attributes.put(CoreAttributes.FILENAME.key(), fileStatusInfo.getFileName());
        attributes.put(CoreAttributes.PATH.key(), fileStatusInfo.getAbsolutePath());
        attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), fileStatusInfo.getAbsolutePath());
        attributes.put(FILE_OWNER_ATTRIBUTE, fileStatusInfo.getOwner());
        attributes.put(FILE_GROUP_ATTRIBUTE, fileStatusInfo.getGroup());
        attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(fileStatusInfo.getModificationTime())));
        attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatter.format(new Date(fileStatusInfo.getAccessTime())));
        attributes.put(FILE_BLOCK_SIZE_ATTRIBUTE, fileStatusInfo.getBlockSize().toString());
        attributes.put(FILE_LENGTH_ATTRIBUTE, fileStatusInfo.getLength().toString());
        return attributes;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected List<FileStatusInfo> performListing(ProcessContext context, final Long minTimestamp) throws IOException {

        final String directory = getPath(context);

        final Boolean recurse = context.getProperty(RECURSE).asBoolean();
        final AzureDataLakeStoreFileSystemServiceInterface adlsService = context.getProperty(ADLS_CLIENT_SERVICE).asControllerService(AzureDataLakeStoreFileSystemServiceInterface.class);
        return scanDirectory(adlsService, directory, directory, fileFilterRef.get(), recurse);
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return DIRECTORY.equals(property)
                || RECURSE.equals(property)
                || FILE_FILTER.equals(property);
    }

    @Override
    protected Scope getStateScope(final ProcessContext context) {
        return Scope.CLUSTER;
    }

    private List<FileStatusInfo> scanDirectory(final AzureDataLakeStoreFileSystemServiceInterface fsService, final String initialDirectory, final String directory, final Pattern fileFilter, final Boolean recurse) throws IOException {
        final List<FileStatusInfo> listing = new ArrayList<>();

        try {
            List<FileStatusProperties> fileStatus = fsService.listFileStatus(directory);

            if (fileStatus != null) {
                for (FileStatusProperties file : fileStatus) {
                    if (file.getType() == FileType.DIRECTORY) {
                        if (recurse) {
                            listing.addAll(scanDirectory(fsService, initialDirectory, combinePath(directory, file.getPathSuffix()), fileFilter, true));
                        }
                    } else if (fileFilter.matcher(file.getPathSuffix()).matches()) {

                        String relativePath;
                        if (initialDirectory.equals(directory)) {
                            relativePath = "./";
                        } else {
                            relativePath = directory.replace(initialDirectory, "");
                        }

                        listing.add(new FileStatusInfo.Builder().fileName(file.getPathSuffix())
                                .absolutePath(directory)
                                .relativePath(relativePath)
                                .modificationTime(file.getModificationTime())
                                .accessTime(file.getAccessTime())
                                .blockSize(file.getBlockSize())
                                .childrenNum(file.getChildrenNum())
                                .group(file.getGroup())
                                .owner(file.getOwner())
                                .length(file.getLength())
                                .permission(file.getPermission())
                                .build());
                    }
                }
            }
        } catch (final CloudException e) {
            throw new IOException("Failed to obtain file listing for " + directory, e);
        }

        return listing;
    }

    private static String combinePath(final String first, final String second) {
        if (first.endsWith("/")) {
            return first + second;
        } else {
            return first + "/" + second;
        }
    }
}
