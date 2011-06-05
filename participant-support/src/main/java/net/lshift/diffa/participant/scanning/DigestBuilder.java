package net.lshift.diffa.participant.scanning;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Utility for building digests based on a series of entities.
 */
public class DigestBuilder {
  private final static Logger log = LoggerFactory.getLogger(DigestBuilder.class);
  private final Map<String, Bucket> digestBuckets;
  private final List<ScanQueryAggregation> aggregations;

  public DigestBuilder(List<ScanQueryAggregation> aggregations) {
    this.aggregations = aggregations;
    this.digestBuckets = new TreeMap<String,Bucket>();
  }

  /**
   * Adds a new version into the builder.
   * @param id the id of the entity being added
   * @param attributes the attributes of the entity being added. The builder expects that an attribute will be present
   *  for each of the aggregations specified.
   * @param vsn the version of the entity
   */
  public void add(String id, Map<String, String> attributes, String vsn) {
    log.trace("Adding to bucket: " + id + ", " + attributes + ", " + vsn);

    Map<String, String> partitions = new HashMap<String, String>();
    StringBuilder labelBuilder = new StringBuilder();
    for (ScanQueryAggregation aggregation : aggregations) {
      String attrVal = attributes.get(aggregation.getAttributeName());
      if (attrVal == null) {
        throw new MissingAttributeException(id, aggregation.getAttributeName());
      }

      String bucket = aggregation.bucket(attrVal);
      partitions.put(aggregation.getAttributeName(), bucket);

      if (labelBuilder.length() > 0) labelBuilder.append("_");
      labelBuilder.append(bucket);
    }

    String label = labelBuilder.toString();
    Bucket bucket = digestBuckets.get(label);
    if (bucket == null) {
      bucket = new Bucket(label, partitions);
      digestBuckets.put(label, bucket);
    }

    bucket.add(vsn);
  }

  public List<QueryResultEntry> toDigests() {
    List<QueryResultEntry> result = new ArrayList<QueryResultEntry>();
    for (Bucket bucket : digestBuckets.values()) {
      result.add(bucket.toDigest());
    }

    return result;
  }

  private static class Bucket {
    private final String name;
    private final Map<String, String> attributes;
    private final String digestAlgorithm = "MD5";
    private final MessageDigest messageDigest;
    private String digest = null;

    public Bucket(String name, Map<String, String> attributes) {
      this.name = name;
      this.attributes = attributes;

      try {
        this.messageDigest = MessageDigest.getInstance(digestAlgorithm);
      } catch (NoSuchAlgorithmException ex) {
        throw new RuntimeException("MD5 digest algorithm no available");
      }
    }

    public void add(String vsn) {
      if (digest != null) {
        throw new SealedBucketException(vsn, name);
      }
      byte[] vsnBytes = vsn.getBytes(Charset.forName("UTF-8"));
      messageDigest.update(vsnBytes, 0, vsnBytes.length);
    }

    public QueryResultEntry toDigest() {
      if (digest == null) {
        digest = new String(Hex.encodeHex(messageDigest.digest()));
      }

      return QueryResultEntry.forAggregate(digest, attributes);
    }
  }
}

