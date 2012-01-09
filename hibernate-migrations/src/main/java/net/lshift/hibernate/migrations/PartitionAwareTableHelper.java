package net.lshift.hibernate.migrations;

import net.lshift.hibernate.migrations.dialects.DialectExtension;

import java.util.List;

/**
 * Abstract class to provide table partition DDL statements.
 */
public class PartitionAwareTableHelper {

  private final DialectExtension dialectExtension;
  private int partitionCount = 1;
  private String[] partitionColumns;

  public PartitionAwareTableHelper(DialectExtension dialectExtension) {
    this.dialectExtension = dialectExtension;
  }

  public void definePartitions(int partitions, String ... columns) {
    if (dialectExtension.supportsHashPartitioning()) {
      partitionCount = partitions;
      partitionColumns = columns;
    }
  }
  
  public void appendPartitionString(StringBuffer buffer) {
    if (dialectExtension.supportsHashPartitioning()) {
      buffer.append(" ");
      buffer.append(dialectExtension.defineHashPartitionString(partitionCount, partitionColumns));
    }
  }

  public void appendAlterFragment(List<String> alterFragments) {
    if (dialectExtension.supportsHashPartitioning()) {
      alterFragments.add()
    }
  }
}
