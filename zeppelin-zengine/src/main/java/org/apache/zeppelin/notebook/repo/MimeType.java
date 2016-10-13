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
package org.apache.zeppelin.notebook.repo;

import net.sf.jmimemagic.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class MimeType {
	private static final Logger LOGGER = LoggerFactory.getLogger(MimeType.class);

	public static final String UNKNOWN_MIME_TYPE="application/x-unknown-mime-type";

	/*
	 Option with mime-util.jar: String mimeType = MimeUtil.getMimeType(file);
   Option with activation.jar: String mimeType = new MimetypesFileTypeMap().getContentType(file);
  */
	public static String getContentType(File file) {

		String extension = new String();
		String s = file.getName();
		int i = s.lastIndexOf('.');
		if (i < s.length() - 1) {
			extension = s.substring(i + 1).toLowerCase();
		}

		if (extension.equals("ogg")) {
			return "application/ogg";
		}

		MagicMatch match = null;
		String mimeType = null;

		try {
			match = Magic.getMagicMatch(file, true);
		} 
		catch (MagicParseException e) {
			LOGGER.warn("Exception while getting MimeType for file: " + file.getAbsolutePath(), e);
		}
		catch (MagicMatchNotFoundException e) {
			LOGGER.warn("Exception while getting MimeType for file: " + file.getAbsolutePath(), e);
		}
		catch (MagicException e) {
			LOGGER.warn("Exception while getting MimeType for file: " + file.getAbsolutePath(), e);
		}
		
		if (match != null) {
			 mimeType = match.getMimeType();
		}
		if (mimeType == null) {
			mimeType = UNKNOWN_MIME_TYPE;	
		}
		
		LOGGER.debug("Found mimetype:" + mimeType + " for file:" + file.getAbsolutePath());
		
		return mimeType;
		
	}

}
