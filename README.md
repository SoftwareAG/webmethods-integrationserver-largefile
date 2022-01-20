# webmethods-integrationserver-largefile
Large File Handling based on Java's NIO technology

# Installation
1) Clone this repository into your git directory.
2) Generate with Eclipse a runnable jar file, e.g. nio.jar
3) Copy the following jar files to your classpath, e.g C:\SoftwareAG\IntegrationServer\instances\default\packages\<your package>\code\jars:
	- nio.jar
	- slf4j-api-1.7.7.jar
	- stringsearch.jar
	
# Usage

- Import tom.Nio
- Create a Nio object, e.g. nio = new Nio()
- Set the necessary Nio params as described in samples
- run nio.splitt(...)

You can either generate the chunks directly in file system or as an output of String[].

# Samples

## Generating chunks using patterns

The following code snippet chunks a large file based on search patterns in IntegrationServer:

		/*
		* Copyright © 2017 - 2018 Software AG, Darmstadt, Germany and/or its licensors
		*
		* SPDX-License-Identifier: Apache-2.0
		*
		*   Licensed under the Apache License, Version 2.0 (the "License");
		*   you may not use this file except in compliance with the License.
		*   You may obtain a copy of the License at
		*
		*       http://www.apache.org/licenses/LICENSE-2.0
		*
		*   Unless required by applicable law or agreed to in writing, software
		*   distributed under the License is distributed on an "AS IS" BASIS,
		*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
		*   See the License for the specific language governing permissions and
		*   limitations under the License.                                                            
		*
		*/

		Nio nio = new Nio();
		FileChannel inChannel = null;
		try {
			inChannel = Nio.prepFiles("split_in/nio-data3.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		boolean hasHeaderBodyTail = false; boolean isFixedLen = false; boolean toDisc = true; boolean useScattering = true; long varLenBufferSize = 2000;
		FileSystem FS = FileSystems.getDefault();
		Path DEST_ROOT_PATH = FS.getPath("split_out");
		nio.addPattern("B3".getBytes(), "\\".getBytes());
		nio.addPattern("C2".getBytes(), null);
		nio.addPattern("A1".getBytes(), null);
		Nio.MAX_FILES_PER_DIR = 5;
		String[] outList = null;
		try {
			outList = nio.splitt(inChannel, DEST_ROOT_PATH, hasHeaderBodyTail, isFixedLen, toDisc, useScattering, null, varLenBufferSize);
			inChannel.close();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// pipeline
		IDataCursor pipelineCursor = pipeline.getCursor();
		String[]	strList = new String[1];
		strList[0] = "strList";
		IDataUtil.put( pipelineCursor, "strList", outList );
		pipelineCursor.destroy();
		
In the above sample the input file "nio-data3.txt" is splitted into chunks using the patterns "B3", "C2", and "A1". "B3" could be escaped with "\" char. In each possible output subdir below "split_out" there will be a maximum of 5 chunk files. The subdir names are numbered from 0 to n. The source file is not a fixed length file (boolean isFixedLen = false).

outList (String[]) is the return param and could have following sample output:

0|3|_prefix_
3|16|B3
19|504|C2
523|444|A1

Which means, that from offset 0 for the length of 3 there is no pattern found, from the offset of 3 for the length of 16 the pattern "B3" was found, and so on ... 
		
## Generating chunks using fixed length

The following code snipped shows the chunking of fixed length files:


	/*
	* Copyright © 2017 - 2018 Software AG, Darmstadt, Germany and/or its licensors
	*
	* SPDX-License-Identifier: Apache-2.0
	*
	*   Licensed under the Apache License, Version 2.0 (the "License");
	*   you may not use this file except in compliance with the License.
	*   You may obtain a copy of the License at
	*
	*       http://www.apache.org/licenses/LICENSE-2.0
	*
	*   Unless required by applicable law or agreed to in writing, software
	*   distributed under the License is distributed on an "AS IS" BASIS,
	*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	*   See the License for the specific language governing permissions and
	*   limitations under the License.                                                            
	*
	*/

	Nio.SRC_PATH = FS.getPath(".");
	System.out.println("SRC_PATH -> "+SRC_PATH.toAbsolutePath());
	Path DEST_ROOT_PATH = FS.getPath("out");
	System.out.println("DEST_ROOT_PATH -> "+DEST_ROOT_PATH.toAbsolutePath());
	FileChannel inChannel = Nio.prepFiles("nio-data.txt");
	boolean hasHeaderBodyTail = true; boolean isFixedLen = true; boolean toDisc = false; boolean useScattering = true; long varLenBufferSize = -1L;
	String[] fileinfo = nio.splitt(inChannel, DEST_ROOT_PATH, hasHeaderBodyTail, isFixedLen, toDisc, useScattering, new long[]{128,256,128}, varLenBufferSize);
	for(String str : fileinfo) {
		System.out.println("str = "+str);
	}
	inChannel.close();

The file "nio-data.txt" is a fixed length file with header/body/trailer parts. The length of the header is 128 bytes, the body parts has a length of 256 bytes each, and the trailer is 128 bytes.

The output looks like this:

str = 1111A1111111111111111B1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
str = A111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
str = 111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111a2222C2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
str = 2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222b2b2C2b22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
str = 22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222b23333E3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333
str = 3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333
str = 33333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333

__________________
These tools are provided as-is and without warranty or support. They do not constitute part of the Software AG product suite. Users are free to use, fork and modify them, subject to the license agreement. While Software AG welcomes contributions, we cannot guarantee to include every contribution in the master project.
__________________
For more information you can Ask a Question in the [TECHcommunity Forums](https://tech.forums.softwareag.com/tags/c/forum/1/webMethods).

You can find additional information in the [Software AG TECHcommunity](https://tech.forums.softwareag.com/tag/webmethods).
