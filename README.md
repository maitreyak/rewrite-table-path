Conversation opened. 1 read message.

Skip to content
Using Gmail with screen readers
praveen 
Iceberg - replace
Inbox

Praveen Tandra
Attachments
Thu, Jul 31, 8:57 AM (1 day ago)
to me

https://github.com/praveentandra/iceberg/tree/s3-path-override-feature
 One attachment
  •  Scanned by Gmail
# S3FileIO Location Override Feature - Implementation Summary

## Problem Statement
Apache Iceberg stores absolute S3 paths in metadata files. During disaster recovery scenarios where data is replicated to a different region/bucket, these absolute paths become invalid, preventing table access when the primary region is down.

## Existing Related Issues & PRs

### Core Issues
- **[#1617](https://github.com/apache/iceberg/issues/1617) - Support Relative Paths in Table Metadata** ⭐
  - Root cause issue requesting relative path support
  - 46+ community reactions
  - Status: Open since 2020, no implementation

- **[#10723](https://github.com/apache/iceberg/issues/10723) - Iceberg Disaster Recovery**
  - Real-world DR scenario with S3 multi-region
  - User experiencing cross-region access failures

- **[#9785](https://github.com/apache/iceberg/issues/9785) - S3FileIO Cross-Region API Calls Failing**
  - Documents S3FileIO limitations with cross-region access

### Related Solutions
- **[PR #10920](https://github.com/apache/iceberg/pull/10920) - RewriteTablePath Action**
  - Post-processing approach to rewrite metadata files
  - Requires explicit action, not runtime transparent

- **[PR #3593](https://github.com/apache/iceberg/pull/3593) - ResolvingFileIO**
  - Pattern for dynamic FileIO selection based on URI scheme
  - Good reference for FileIO wrapper patterns

## Proposed Solution: S3FileIO Path Override

### Design Overview
Implement a runtime path translation layer in S3FileIO that transparently remaps S3 paths without modifying metadata files.

### Key Benefits
- **Transparent**: No metadata modification required
- **Runtime**: Works immediately during failover
- **Backward Compatible**: Opt-in via configuration
- **Flexible**: Supports multiple mapping rules

## Implementation Architecture

### 1. Configuration Properties
```properties
# Enable path override
s3.path-override.enabled=true

# Define mappings
s3.path-override.mapping.1.source=s3://east-bucket/warehouse/
s3.path-override.mapping.1.target=s3://west-bucket/warehouse/
```

### 2. Core Components

#### A. S3PathOverrideResolver (New Class)
```java
package org.apache.iceberg.aws.s3;

import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3PathOverrideResolver {
    private static final Logger LOG = LoggerFactory.getLogger(S3PathOverrideResolver.class);
    
    private final boolean enabled;
    private final Map<String, PathMapping> mappings;
    
    public static class PathMapping {
        private final String sourcePrefix;
        private final String targetPrefix;
        
        PathMapping(String sourcePrefix, String targetPrefix) {
            this.sourcePrefix = normalizePrefix(sourcePrefix);
            this.targetPrefix = normalizePrefix(targetPrefix);
        }
        
        private static String normalizePrefix(String prefix) {
            return prefix.endsWith("/") ? prefix : prefix + "/";
        }
        
        boolean matches(String path) {
            return path.startsWith(sourcePrefix);
        }
        
        String remap(String path) {
            return targetPrefix + path.substring(sourcePrefix.length());
        }
    }
    
    public S3PathOverrideResolver(Map<String, String> properties) {
        this.enabled = Boolean.parseBoolean(
            properties.getOrDefault(S3FileIOProperties.S3_PATH_OVERRIDE_ENABLED, "false"));
        this.mappings = parseMappings(properties);
    }
    
    private Map<String, PathMapping> parseMappings(Map<String, String> properties) {
        Map<String, PathMapping> result = new TreeMap<>();
        
        properties.entrySet().stream()
            .filter(e -> e.getKey().startsWith(S3FileIOProperties.S3_PATH_OVERRIDE_PREFIX))
            .forEach(e -> {
                String key = e.getKey();
                if (key.endsWith(".source")) {
                    String mappingId = key.substring(
                        S3FileIOProperties.S3_PATH_OVERRIDE_PREFIX.length(),
                        key.length() - ".source".length());
                    String targetKey = S3FileIOProperties.S3_PATH_OVERRIDE_PREFIX + mappingId + ".target";
                    String targetValue = properties.get(targetKey);
                    
                    if (targetValue != null) {
                        result.put(e.getValue(), new PathMapping(e.getValue(), targetValue));
                    }
                }
            });
        
        return result;
    }
    
    public String resolvePath(String originalPath) {
        if (!enabled || originalPath == null) {
            return originalPath;
        }
        
        // Find the first matching mapping (ordered by source prefix)
        for (PathMapping mapping : mappings.values()) {
            if (mapping.matches(originalPath)) {
                String remappedPath = mapping.remap(originalPath);
                LOG.debug("Remapped path from {} to {}", originalPath, remappedPath);
                return remappedPath;
            }
        }
        
        return originalPath;
    }
}
```

#### B. S3FileIO Modifications
```java
public class S3FileIO implements FileIO {
    // Add new field
    private S3PathOverrideResolver pathOverrideResolver;
    
    // Modify initialize method
    @Override
    public void initialize(Map<String, String> props) {
        this.properties = SerializableMap.copyOf(props);
        this.pathOverrideResolver = new S3PathOverrideResolver(props);
        // ... existing initialization code ...
    }
    
    // Modify newInputFile methods
    @Override
    public InputFile newInputFile(String path) {
        String resolvedPath = pathOverrideResolver.resolvePath(path);
        return S3InputFile.fromLocation(resolvedPath, clientForStoragePath(resolvedPath), metrics);
    }
    
    @Override
    public InputFile newInputFile(String path, long length) {
        String resolvedPath = pathOverrideResolver.resolvePath(path);
        return S3InputFile.fromLocation(resolvedPath, length, clientForStoragePath(resolvedPath), metrics);
    }
    
    // Modify newOutputFile method
    @Override
    public OutputFile newOutputFile(String path) {
        String resolvedPath = pathOverrideResolver.resolvePath(path);
        return S3OutputFile.fromLocation(resolvedPath, clientForStoragePath(resolvedPath), metrics);
    }
    
    // Modify deleteFile methods
    @Override
    public void deleteFile(String path) {
        String resolvedPath = pathOverrideResolver.resolvePath(path);
        // ... existing delete logic with resolvedPath ...
    }
}
```

#### C. S3FileIOProperties Additions
```java
// New properties in S3FileIOProperties.java
public static final String S3_PATH_OVERRIDE_ENABLED = "s3.path-override.enabled";
public static final String S3_PATH_OVERRIDE_PREFIX = "s3.path-override.mapping.";
```

### 3. Integration Points
- **Initialize**: Parse mappings from properties during S3FileIO initialization
- **File Operations**: Apply path resolution to all FileIO methods
- **Logging**: Debug-level logging for path translations

## Implementation Checklist

### Code Changes
- [ ] Add configuration properties to `S3FileIOProperties.java`
- [ ] Create `S3PathOverrideResolver.java` class
- [ ] Modify `S3FileIO.java` to integrate resolver
- [ ] Update `S3InputFile.java` and `S3OutputFile.java` for consistency

### Testing
- [ ] Unit tests for `S3PathOverrideResolver`
- [ ] Integration tests for S3FileIO with path override
- [ ] Cross-region failover scenario tests
- [ ] Performance benchmarks

### Documentation
- [ ] Update S3FileIO documentation
- [ ] Add configuration examples
- [ ] Document DR use cases
- [ ] Migration guide from RewriteTablePath

## Test Examples

### Unit Test for S3PathOverrideResolver
```java
@Test
public void testSimplePathRemapping() {
    Map<String, String> props = ImmutableMap.of(
        S3FileIOProperties.S3_PATH_OVERRIDE_ENABLED, "true",
        S3FileIOProperties.S3_PATH_OVERRIDE_PREFIX + "1.source", "s3://source-bucket/data/",
        S3FileIOProperties.S3_PATH_OVERRIDE_PREFIX + "1.target", "s3://target-bucket/data/"
    );
    
    S3PathOverrideResolver resolver = new S3PathOverrideResolver(props);
    assertEquals("s3://target-bucket/data/file.parquet", 
                 resolver.resolvePath("s3://source-bucket/data/file.parquet"));
}

@Test
public void testDisabledOverride() {
    Map<String, String> props = ImmutableMap.of(
        S3FileIOProperties.S3_PATH_OVERRIDE_ENABLED, "false"
    );
    
    S3PathOverrideResolver resolver = new S3PathOverrideResolver(props);
    assertEquals("s3://source-bucket/data/file.parquet", 
                 resolver.resolvePath("s3://source-bucket/data/file.parquet"));
}

@Test
public void testMultipleMappings() {
    Map<String, String> props = ImmutableMap.of(
        S3FileIOProperties.S3_PATH_OVERRIDE_ENABLED, "true",
        S3FileIOProperties.S3_PATH_OVERRIDE_PREFIX + "1.source", "s3://bucket-a/",
        S3FileIOProperties.S3_PATH_OVERRIDE_PREFIX + "1.target", "s3://bucket-b/",
        S3FileIOProperties.S3_PATH_OVERRIDE_PREFIX + "2.source", "s3://bucket-c/",
        S3FileIOProperties.S3_PATH_OVERRIDE_PREFIX + "2.target", "s3://bucket-d/"
    );
    
    S3PathOverrideResolver resolver = new S3PathOverrideResolver(props);
    assertEquals("s3://bucket-b/data/file1.parquet", 
                 resolver.resolvePath("s3://bucket-a/data/file1.parquet"));
    assertEquals("s3://bucket-d/data/file2.parquet", 
                 resolver.resolvePath("s3://bucket-c/data/file2.parquet"));
}
```

## Use Cases

### 1. Disaster Recovery
```properties
# Primary region down, failover to DR region
s3.path-override.enabled=true
s3.path-override.mapping.1.source=s3://us-east-1-bucket/
s3.path-override.mapping.1.target=s3://us-west-2-bucket/
```

### 2. Bucket Migration
```properties
# Gradual migration from old to new bucket
s3.path-override.enabled=true
s3.path-override.mapping.1.source=s3://old-bucket/
s3.path-override.mapping.1.target=s3://new-bucket/
```

### 3. Multi-Region Read Replicas
```properties
# Route reads to nearest region
s3.path-override.enabled=true
s3.path-override.mapping.1.source=s3://global-bucket/
s3.path-override.mapping.1.target=s3://regional-bucket/
```

## Comparison with Alternatives

| Approach | Pros | Cons |
|----------|------|------|
| **Path Override (This PR)** | • Transparent at runtime<br>• No metadata changes<br>• Immediate failover | • Configuration required<br>• Not a permanent solution |
| **Relative Paths (#1617)** | • Permanent solution<br>• No configuration needed | • Major spec change<br>• Not implemented yet |
| **RewriteTablePath (#10920)** | • Already available<br>• Permanent fix | • Requires downtime<br>• Modifies metadata files |
| **MRAP** | • AWS-native solution<br>• Automatic failover | • Requires setup upfront<br>• AWS-specific |

## Documentation Example

```markdown
## S3FileIO Path Override Configuration

S3FileIO supports path override for disaster recovery scenarios where 
metadata contains absolute paths pointing to a different S3 location.

### Configuration Properties

- `s3.path-override.enabled`: Enable path override feature (default: false)
- `s3.path-override.mapping.{n}.source`: Source path prefix to match
- `s3.path-override.mapping.{n}.target`: Target path prefix to replace with

### Example: Cross-Region Failover

```properties
# Enable path override for disaster recovery
s3.path-override.enabled=true

# Map East region bucket to West region bucket
s3.path-override.mapping.1.source=s3://us-east-1-bucket/warehouse/
s3.path-override.mapping.1.target=s3://us-west-2-bucket/warehouse/
```

When enabled, S3FileIO will transparently remap all file operations 
from the source to target locations without modifying table metadata.
```

## Performance Considerations

- Path resolution adds minimal overhead (simple string prefix matching)
- Mappings are parsed once during initialization
- No additional S3 API calls required
- Debug logging available for troubleshooting

## Future Enhancements

1. **Regex-based matching**: Support pattern-based remapping
2. **Dynamic reloading**: Allow mapping changes without restart
3. **Metrics**: Track remapping statistics
4. **Validation**: Verify target paths are accessible during initialization

## Next Steps

1. **Community Engagement**
   - Comment on issue #10723 with proposed solution
   - Reference this as tactical fix while #1617 is strategic solution
   - Gather feedback on approach

2. **Implementation**
   - Fork Apache Iceberg repository
   - Implement code changes
   - Write comprehensive tests
   - Submit PR with references to related issues

3. **Documentation**
   - Create user guide for DR scenarios
   - Document performance implications
   - Provide migration examples

## Summary
This S3FileIO path override feature provides an immediate, runtime solution for disaster recovery scenarios while the Iceberg community works on long-term relative path support. It's designed to be simple, backward-compatible, and addresses real-world DR challenges faced by multiple users in the community.
iceberg-s3-path-override-proposal.md
Displaying iceberg-s3-path-override-proposal.md.
