/*
 * Copyright 2016 Datalayer (http://datalayer.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.rest;

import org.apache.zeppelin.notebook.repo.MimeContent;
import org.apache.zeppelin.notebook.repo.NotebookRepo;
import org.apache.zeppelin.server.ZeppelinServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Date;

@Path("/content")
public class ContentRestApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContentRestApi.class);
	private final int chunk_size = 1024 * 1024; // 1MB chunks

	private NotebookRepo notebookRepo;

	public ContentRestApi() {}

	public ContentRestApi(NotebookRepo notebookRepo) {
		this.notebookRepo = notebookRepo;
	}

	@GET
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("{noteId}/{contentId}")
	public Response streamContent(@Context HttpServletRequest request,
																 @Context HttpServletResponse response,
																 @PathParam("noteId") String noteId,
																 @PathParam("contentId") String contentId,
	                               @HeaderParam("Range") String range)
          throws Exception {

		final MimeContent mimeContent = this.notebookRepo.read(noteId, contentId);

		if (range == null) {
			StreamingOutput streamer = new StreamingOutput() {
				@Override
				public void write(final OutputStream output) throws IOException, WebApplicationException {
					final FileChannel inputChannel = new FileInputStream(mimeContent.file).getChannel();
					final WritableByteChannel outputChannel = Channels.newChannel(output);
					try {
						inputChannel.transferTo(0, inputChannel.size(), outputChannel);
					} finally {
						// closing the channels
						inputChannel.close();
						outputChannel.close();
					}
				}
			};
      LOGGER.debug("Streaming with FileChannel: " + mimeContent.file.getAbsolutePath());;
      return Response
							.ok(streamer)
							.status(200)
							.header("Accept-Ranges", "bytes")
							.header(HttpHeaders.CONTENT_TYPE, mimeContent.mimeType)
              .header(HttpHeaders.CONTENT_LENGTH, mimeContent.file.length())
              .header(HttpHeaders.LAST_MODIFIED, new Date(mimeContent.file.lastModified()))
//              .header("Cache-Control", "no-cache, no-store, must-revalidate") // HTTP 1.1.
//              .header("Pragma", "no-cache") // HTTP 1.0.
//              .header("Expires", "0") // Proxies.
           		.build()
              ;
		}

		String[] ranges = range.split("=")[1].split("-");
		final int from = Integer.parseInt(ranges[0]);

		int to = from + chunk_size;
		if (to >= mimeContent.file.length()) {
			to = (int) (mimeContent.file.length() - 1);
		}

    // TODO Force the to begin the end of the file???
    to = (int) (mimeContent.file.length() - 1);

		if (ranges.length == 2) {
			to = Integer.parseInt(ranges[1]);
		}

		final String responseRange = String.format("bytes %d-%d/%d", from, to, mimeContent.file.length());
		final RandomAccessFile raf = new RandomAccessFile(mimeContent.file, "r");
		raf.seek(from);

		final int len = to - from + 1;
		final MediaStreamer streamer = new MediaStreamer(len, raf);

    LOGGER.info("Streaming with ranges: " + responseRange + " - Length: " + len + " - File: " + mimeContent.file.getAbsolutePath());

    return Response
						.ok(streamer)
						.status(206)
						.header("Accept-Ranges", "bytes")
						.header("Content-Range", responseRange)
						.header(HttpHeaders.CONTENT_TYPE, mimeContent.mimeType)
						.header(HttpHeaders.CONTENT_LENGTH, streamer.getLength())
						.header(HttpHeaders.LAST_MODIFIED, new Date(mimeContent.file.lastModified()))
//            .header("Cache-Control", "no-cache, no-store, must-revalidate") // HTTP 1.1.
//            .header("Pragma", "no-cache") // HTTP 1.0.
//            .header("Expires", "0") // Proxies.
						.build()
						;

	}

}
