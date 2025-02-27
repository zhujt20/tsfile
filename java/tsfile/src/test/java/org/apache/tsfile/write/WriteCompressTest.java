/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tsfile.write;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class WriteCompressTest {
  private static final String SENSOR = "sensor_";
  private static final String DEVICE = "device";
  private final List<List<IMeasurementSchema>> deviceList = new ArrayList<>();
  private static final DecimalFormat formater = new DecimalFormat("#,###.##");

  @Test
  public void testWrite() throws IOException, WriteProcessException {
    for (CompressionType type : CompressionType.values()) {
      try {
        writeTest(type);
      } catch (IOException | WriteProcessException e) {
      }
    }
  }

  @Test
  public void testRead() throws IOException, WriteProcessException {
    for (CompressionType type : CompressionType.values()) {
      try {
        readTest(type);
      } catch (IOException e) {
      }
    }
  }

  public void readTest(CompressionType type) throws IOException {
    String TSFILE_PATH = type.toString() + "tsfile";
    TsFileSequenceReader fileReader = new TsFileSequenceReader(TSFILE_PATH);
    TsFileReader reader = new TsFileReader(fileReader);
    List<Path> pathList = new ArrayList<>();
    int num = 0;
    for (int j = 0; j < 50; j++) {
      for (int i = 0; i < 50; i++) {
        pathList.add(new Path(DEVICE + j, SENSOR + i, true));
        num++;
      }
    }
    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    long startTime = System.currentTimeMillis();
    QueryDataSet dataSet = reader.query(queryExpression);
    long count = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      count++;
    }
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("query all cost: " + (duration / 1000.0) + "s");
    System.out.println("query get points: " + formater.format(count * num) + "points");
    System.out.println(
        "query speed : " + formater.format(count * num / (duration / 1000.0)) + "points/s");
  }

  @Test
  public void testAlignedRead() throws IOException, WriteProcessException {
    for (CompressionType type : CompressionType.values()) {
      try {
        readAlignedTest(type);
        break;
      } catch (IOException e) {
      }
    }
  }

  public void readAlignedTest(CompressionType type) throws IOException {
    String TSFILE_PATH = type.toString() + "aligned" + "tsfile";
    TsFileSequenceReader fileReader = new TsFileSequenceReader(TSFILE_PATH);
    TsFileReader reader = new TsFileReader(fileReader);
    List<Path> pathList = new ArrayList<>();
    int num = 0;
    for (int j = 0; j < 50; j++) {
      for (int i = 0; i < 50; i++) {
        pathList.add(new Path(DEVICE + j, SENSOR + i, true));
        num++;
      }
    }
    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    long startTime = System.currentTimeMillis();
    QueryDataSet dataSet = reader.query(queryExpression);
    long count = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      count++;
    }
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("query all cost: " + (duration / 1000.0) + "s");
    System.out.println("query get points: " + formater.format(count * num) + "points");
    System.out.println(
        "query speed : " + formater.format(count * num / (duration / 1000.0)) + "points/s");
  }

  @Test
  public void testAlignedWrite() throws IOException, WriteProcessException {
    for (CompressionType type : CompressionType.values()) {
      try {
        writeAlignedTest(type);
        break;
      } catch (IOException | WriteProcessException e) {
      }
    }
  }

  private void writeAlignedTest(CompressionType compressionType)
      throws IOException, WriteProcessException {
    String path = compressionType.toString() + "aligned" + "tsfile";
    File f = FSFactoryProducer.getFSFactory().getFile(path);
    if (f.exists()) {
      Files.delete(f.toPath());
    }
    int deviceNum = 50;
    int measurementNum = 50;
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementNum; i++) {
        measurementSchemas.add(
            new MeasurementSchema(
                "sensor_" + i, TSDataType.INT32, TSEncoding.TS_2DIFF, compressionType));
      }

      String device_name = "device";
      for (int i = 0; i < deviceNum; i++) {
        String deviceId = device_name + i;
        tsFileWriter.registerAlignedTimeseries(deviceId, measurementSchemas);
      }
      long start = System.currentTimeMillis();
      int max_rows = 1000000;
      int tablet_size = 100000;
      int cur = 0;
      System.out.println("start");
      for (; cur < max_rows; ) {
        if (cur + tablet_size > max_rows) {
          tablet_size = max_rows - cur;
        }
        for (int i = 0; i < deviceNum; i++) {
          String deviceId = device_name + i;
          Tablet tablet = new Tablet(deviceId, measurementSchemas, tablet_size);
          tablet.initBitMaps();
          for (int row = 0; row < tablet_size; row++) {
            tablet.addTimestamp(row, 12345 + cur + row);
          }
          for (int j = 0; j < measurementNum; j++) {
            for (int row = 0; row < tablet_size; row++) {
              tablet.addValue(measurementSchemas.get(j).getMeasurementName(), row, cur + row);
            }
          }
          tsFileWriter.writeAligned(tablet);
          tsFileWriter.flush();
        }
        System.out.println("cur write:" + cur);
        cur += tablet_size;
      }
      long end = System.currentTimeMillis();
      System.out.println("write cost:" + (end - start) + "ms");
    }
    long fileSize = Files.size(f.toPath());
    System.out.println("file size:" + fileSize);
    System.out.print("write finish" + compressionType.toString());
  }

  private void writeTest(CompressionType compressionType)
      throws IOException, WriteProcessException {
    String path = compressionType.toString() + "tsfile";
    File f = FSFactoryProducer.getFSFactory().getFile(path);
    if (f.exists()) {
      Files.delete(f.toPath());
    }
    int deviceNum = 50;
    int measurementNum = 50;
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      for (int i = 0; i < measurementNum; i++) {
        measurementSchemas.add(
            new MeasurementSchema(
                "sensor_" + i, TSDataType.INT32, TSEncoding.TS_2DIFF, compressionType));
      }
      String device_name = "device";
      for (int i = 0; i < deviceNum; i++) {
        String deviceId = device_name + i;
        tsFileWriter.registerTimeseries(new Path(deviceId), measurementSchemas);
      }
      long start = System.currentTimeMillis();
      int max_rows = 1000000;
      int tablet_size = 100000;
      int cur = 0;
      System.out.println("start");
      for (; cur < max_rows; ) {
        if (cur + tablet_size > max_rows) {
          tablet_size = max_rows - cur;
        }
        for (int i = 0; i < deviceNum; i++) {
          String deviceId = device_name + i;
          Tablet tablet = new Tablet(deviceId, measurementSchemas, tablet_size);
          tablet.initBitMaps();
          for (int row = 0; row < tablet_size; row++) {
            tablet.addTimestamp(row, 12345 + cur + row);
          }
          for (int j = 0; j < measurementNum; j++) {
            for (int row = 0; row < tablet_size; row++) {
              tablet.addValue(measurementSchemas.get(j).getMeasurementName(), row, cur + row);
            }
          }
          //                    System.out.println("tablet init finsh");
          tsFileWriter.writeTree(tablet);
          tsFileWriter.flush();
        }
        System.out.println("cur write:" + cur);
        cur += tablet_size;
      }
      long end = System.currentTimeMillis();
      System.out.println("write cost:" + (end - start) + "ms");
    }
    long fileSize = Files.size(f.toPath());
    System.out.println("file size:" + fileSize);
    System.out.print("write finish" + compressionType.toString());
  }

  @Test
  public void testCsvWrite() throws IOException, WriteProcessException {
    String path = "output.csv";
    File f = new File(path);
    if (f.exists()) {
      Files.delete(f.toPath());
    }
    int deviceNum = 50;
    int measurementNum = 50;
    try (BufferedWriter writer = Files.newBufferedWriter(f.toPath())) {
      StringBuilder header = new StringBuilder("time,deviceId");
      for (int j = 1; j <= measurementNum; j++) {
        header.append(",s").append(j); // 添加s1, s2, ..., s50
      }
      writer.write(header.toString());
      writer.newLine();
      long start = System.currentTimeMillis();
      int max_rows = 1000000;
      int tablet_size = 100000;
      int cur = 0;
      System.out.println("start");
      for (; cur < max_rows; ) {
        if (cur + tablet_size > max_rows) {
          tablet_size = max_rows - cur;
        }
        for (int i = 0; i < deviceNum; i++) {
          for (int row = 0; row < tablet_size; row++) {
            StringBuilder line = new StringBuilder();
            line.append(12345 + cur + row);
            String deviceId = "device" + i;
            line.append(",").append(deviceId);
            for (int j = 0; j < measurementNum; j++) {
              line.append(",").append(cur + row); // 假设数据值为 cur + row
            }
            writer.write(line.toString());
            writer.newLine();
          }
        }
        System.out.println("cur write:" + cur);
        cur += tablet_size;
      }
      long end = System.currentTimeMillis();
      System.out.println("write cost:" + (end - start) + "ms");
    }
    long fileSize = Files.size(f.toPath());
    System.out.println("file size:" + fileSize);
    System.out.print("write finish");
  }

  @Test
  public void testCsvRead() throws IOException {
    // 1. 查询某个设备的某个传感器的数据
    String deviceId = "device1"; // 假设查询 device1
    String sensorId = "s1"; // 假设查询 s1
    queryDeviceSensorData(deviceId, sensorId);

    // 2. 查询某个设备的所有传感器的数据
    queryDeviceData(deviceId);

    // 3. 查询所有设备的所有数据
    queryAllData();
  }

  // 查询某个设备的某个传感器的全量数据
  public void queryDeviceSensorData(String deviceId, String sensorId) throws IOException {
    try {
      BufferedReader reader = Files.newBufferedReader(Paths.get("output.csv"));
      String line;
      long count = 0;
      long startTime = System.currentTimeMillis();
      String Header = reader.readLine();
      String[] headers = Header.split(",");
      while ((line = reader.readLine()) != null) {
        String[] columns = line.split(",");
        if (columns[1].equals(deviceId)) {
          for (int i = 2; i < headers.length; i++) {
            if (headers[i].equals(sensorId)) {
              count++;
            }
          }
        }
      }
      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;
      System.out.println(
          "Query device "
              + deviceId
              + " sensor "
              + sensorId
              + " cost: "
              + (duration / 1000.0)
              + "s");
      System.out.println("Query points: " + formater.format(count) + " points");
      System.out.println(
          "Query speed: " + formater.format(count / (duration / 1000.0)) + " points/s");
    } catch (IOException e) {
    }
  }

  // 查询某个设备的全量数据
  public void queryDeviceData(String deviceId) throws IOException {
    try {
      BufferedReader reader = Files.newBufferedReader(Paths.get("output.csv"));
      String line;
      long count = 0;
      long startTime = System.currentTimeMillis();

      // Skip header line
      String Header = reader.readLine();
      String[] headers = Header.split(",");

      while ((line = reader.readLine()) != null) {
        String[] columns = line.split(",");

        // 过滤条件：匹配特定设备
        if (columns[1].equals(deviceId)) {
          for (int i = 2; i < headers.length; i++) {
            count++;
          }
        }
      }

      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;

      System.out.println("Query device " + deviceId + " cost: " + (duration / 1000.0) + "s");
      System.out.println("Query points: " + formater.format(count) + " points");
      System.out.println(
          "Query speed: " + formater.format(count / (duration / 1000.0)) + " points/s");
    } catch (IOException e) {
    }
  }

  // 查询所有数据
  public void queryAllData() throws IOException {
    try {
      BufferedReader reader = Files.newBufferedReader(Paths.get("output.csv"));
      String line;
      long count = 0;
      long startTime = System.currentTimeMillis();

      // Skip header line
      String Header = reader.readLine();
      String[] headers = Header.split(",");

      while ((line = reader.readLine()) != null) {
        String[] columns = line.split(",");

        // 过滤条件：无
        for (int i = 2; i < headers.length; i++) {
          count++;
        }
      }

      long endTime = System.currentTimeMillis();
      long duration = endTime - startTime;

      System.out.println("Query all " + " cost: " + (duration / 1000.0) + "s");
      System.out.println("Query points: " + formater.format(count) + " points");
      System.out.println(
          "Query speed: " + formater.format(count / (duration / 1000.0)) + " points/s");
    } catch (IOException e) {
    }
  }
}
