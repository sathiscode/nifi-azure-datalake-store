/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.nifi.processors.azure.datalake.store.model;

import com.microsoft.azure.management.datalake.store.models.FileType;
import java.io.Serializable;
import org.apache.nifi.processors.standard.util.ListableEntity;

public class FileStatusInfo implements Comparable<FileStatusInfo>, Serializable, ListableEntity {

    private static final long serialVersionUID = 1L;

    private final String fileName;
    private final String absolutePath;
    private final String realtivePath;
    private final FileType fileType;
    private final Long accessTime;
    private final Long blockSize;
    private final Long childrenNum;
    private final String group;
    private final Long length;
    private final Long modificationTime;
    private final String owner;
    private final String permission;

    public String getFileName() {
        return fileName;
    }

    public String getAbsolutePath() {
        return absolutePath;
    }

    public String getRelativePath() {
        return realtivePath;
    }

    public Long getAccessTime() {
        return accessTime;
    }

    public Long getBlockSize() {
        return blockSize;
    }

    public Long getChildrenNum() {
        return childrenNum;
    }

    public String getGroup() {
        return group;
    }

    public Long getLength() {
        return length;
    }

    public Long getModificationTime() {
        return modificationTime;
    }

    public String getOwner() {
        return owner;
    }

    public String getPermission() {
        return permission;
    }

    public FileType getFileType() {
        return fileType;
    }

    protected FileStatusInfo(final Builder builder) {
        this.fileName = builder.fileName;
        this.absolutePath = builder.absolutePath;
        this.realtivePath = builder.relativePath;
        this.accessTime = builder.accessTime;
        this.blockSize = builder.blockSize;
        this.childrenNum = builder.childrenNum;
        this.group = builder.group;
        this.length = builder.length;
        this.modificationTime = builder.modificationTime;
        this.owner = builder.owner;
        this.permission = builder.permission;
        this.fileType = builder.fileType;
    }

    public static final class Builder {

        private String fileName;
        private String absolutePath;
        private String relativePath;
        private FileType fileType;
        private Long accessTime;
        private Long blockSize;
        private Long childrenNum;
        private String group;
        private Long length;
        private Long modificationTime;
        private String owner;
        private String permission;

        public FileStatusInfo build() {
            return new FileStatusInfo(this);
        }

        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder absolutePath(String absolutePath) {
            this.absolutePath = absolutePath;
            return this;
        }

        public Builder relativePath(String realtivePath) {
            this.relativePath = realtivePath;
            return this;
        }

        public Builder accessTime(Long accessTime) {
            this.accessTime = accessTime;
            return this;
        }

        public Builder blockSize(Long blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder childrenNum(Long childrenNum) {
            this.childrenNum = childrenNum;
            return this;
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder length(Long length) {
            this.length = length;
            return this;
        }

        public Builder modificationTime(Long modificationTime) {
            this.modificationTime = modificationTime;
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder permission(String permission) {
            this.permission = permission;
            return this;
        }

        public Builder fileType(FileType type) {
            this.fileType = type;
            return this;
        }
    }

    @Override
    public int compareTo(FileStatusInfo o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getName() {
        return getAbsolutePath();
    }

    @Override
    public String getIdentifier() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public long getTimestamp() {
        return getModificationTime();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fileName == null) ? 0 : fileName.hashCode()) + ((absolutePath == null) ? 0 : absolutePath.hashCode()) + ((owner == null) ? 0 : owner.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FileStatusInfo other = (FileStatusInfo) obj;
        if (fileName == null) {
            if (other.fileName != null) {
                return false;
            }
        } else if (!fileName.equals(other.fileName)) {
            return false;
        } else if (absolutePath == null) {
            if (other.absolutePath != null) {
                return false;
            }
        } else if (!absolutePath.equals(other.absolutePath)) {
            return false;
        } else if (!blockSize.equals(other.blockSize)) {
            return false;
        } else if (!owner.equals(other.owner)) {
            return false;
        } else if (!childrenNum.equals(other.childrenNum)) {
            return false;
        } else if (!fileType.equals(other.fileType)) {
            return false;
        }
        return true;
    }

}
