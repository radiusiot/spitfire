/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

public class MediaStreamer implements StreamingOutput {
  private static final Logger LOGGER = LoggerFactory.getLogger(MediaStreamer.class);
  private int length;
  private RandomAccessFile raf;
  final byte[] buf = new byte[4096];

  public MediaStreamer(int length, RandomAccessFile raf) {
    this.length = length;
    this.raf = raf;
  }

  @Override
  public void write(OutputStream outputStream) throws IOException, WebApplicationException {
    try {
      while (length != 0) {
        int read = raf.read(buf, 0, buf.length > length ? length : buf.length);
        outputStream.write(buf, 0, read);
        length -= read;
//        LOGGER.trace("Read: " + read + " - Length: " + length);
      }
//      LOGGER.trace("Content written to OutputStream...");
    }
    catch(Exception e) {
      LOGGER.warn("Exception while writing the content stream." , e);
    } finally {
      raf.close();
//      outputStream.flush();
//      outputStream.close();
    }
  }

  public int getLength() {
    return length;
  }

}
