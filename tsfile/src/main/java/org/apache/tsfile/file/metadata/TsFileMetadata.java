/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.utils.BloomFilter;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/** TSFileMetaData collects all metadata info and saves in its data structure. */
public class TsFileMetadata {

  // bloom filter
  private BloomFilter bloomFilter;

  // List of <name, offset, childMetadataIndexType>
  private Map<String, MetadataIndexNode> tableMetadataIndexNodeMap;
  private Map<String, TableSchema> tableSchemaMap;

  // offset of MetaMarker.SEPARATOR
  private long metaOffset;

  /**
   * deserialize data from the buffer.
   *
   * @param buffer -buffer use to deserialize
   * @return -a instance of TsFileMetaData
   */
  public static TsFileMetadata deserializeFrom(ByteBuffer buffer) {
    TsFileMetadata fileMetaData = new TsFileMetadata();

    // metadataIndex
    int tableIndexNodeNum = buffer.getInt();
    Map<String, MetadataIndexNode> tableIndexNodeMap = new HashMap<>();
    for (int i = 0; i < tableIndexNodeNum; i++) {
      String tableName = ReadWriteIOUtils.readString(buffer);
      MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer, true);
      tableIndexNodeMap.put(tableName, metadataIndexNode);
    }
    fileMetaData.setTableMetadataIndexNodeMap(tableIndexNodeMap);

    // tableSchemas
    int tableSchemaNum = buffer.getInt();
    Map<String, TableSchema> tableSchemaMap = new HashMap<>();
    for (int i = 0; i < tableSchemaNum; i++) {
      String tableName = ReadWriteIOUtils.readString(buffer);
      TableSchema tableSchema = TableSchema.deserialize(tableName, buffer);
      tableSchemaMap.put(tableName, tableSchema);
    }
    fileMetaData.setTableSchemaMap(tableSchemaMap);

    // metaOffset
    long metaOffset = ReadWriteIOUtils.readLong(buffer);
    fileMetaData.setMetaOffset(metaOffset);

    // read bloom filter
    if (buffer.hasRemaining()) {
      byte[] bytes = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
      int filterSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      int hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      fileMetaData.bloomFilter = BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize);
    }

    return fileMetaData;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  public void setBloomFilter(BloomFilter bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  /**
   * use the given outputStream to serialize.
   *
   * @param outputStream -output stream to determine byte length
   * @return -byte length
   * @throws IOException error when operating outputStream
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    if (tableMetadataIndexNodeMap != null) {
      byteLen += ReadWriteIOUtils.write(tableMetadataIndexNodeMap.size(), outputStream);
      for (Entry<String, MetadataIndexNode> entry : tableMetadataIndexNodeMap.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
        byteLen += entry.getValue().serializeTo(outputStream);
      }
    } else {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    }

    if (tableSchemaMap != null) {
      byteLen += ReadWriteIOUtils.write(tableSchemaMap.size(), outputStream);
      for (Entry<String, TableSchema> entry : tableSchemaMap.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
        byteLen += entry.getValue().serialize(outputStream);
      }
    } else {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    }

    // metaOffset
    byteLen += ReadWriteIOUtils.write(metaOffset, outputStream);

    return byteLen;
  }

  public int serializeBloomFilter(OutputStream outputStream, BloomFilter filter)
      throws IOException {
    int byteLen = 0;
    byte[] bytes = filter.serialize();
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(bytes.length, outputStream);
    outputStream.write(bytes);
    byteLen += bytes.length;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getSize(), outputStream);
    byteLen +=
        ReadWriteForEncodingUtils.writeUnsignedVarInt(filter.getHashFunctionSize(), outputStream);
    return byteLen;
  }

  public long getMetaOffset() {
    return metaOffset;
  }

  public void setMetaOffset(long metaOffset) {
    this.metaOffset = metaOffset;
  }

  public void setTableMetadataIndexNodeMap(
      Map<String, MetadataIndexNode> tableMetadataIndexNodeMap) {
    this.tableMetadataIndexNodeMap = tableMetadataIndexNodeMap;
  }

  public void setTableSchemaMap(Map<String, TableSchema> tableSchemaMap) {
    this.tableSchemaMap = tableSchemaMap;
  }

  public Map<String, MetadataIndexNode> getTableMetadataIndexNodeMap() {
    return tableMetadataIndexNodeMap;
  }

  public Map<String, TableSchema> getTableSchemaMap() {
    return tableSchemaMap;
  }
}