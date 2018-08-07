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


import java.nio.ByteBuffer;

import tom.Nio;


public class BufferHandlerSampleImpl implements tom.BufferHandlerInterface {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleBuffer(Nio nio, ByteBuffer bb) {
		// TODO Auto-generated method stub
		System.out.println("*** start handle byte buffer here! ***");
		nio.printBuffer(bb);
		System.out.println();
		long pos = nio.search(bb.array(), 0, bb.array().length, "1a".getBytes());
		System.out.println("pos of '1a' is "+pos);
		if (pos > 0) {
			nio.storeMetaInfo("key1", bb, (int) pos, "1a".length());
		}
		System.out.println("*** end handle byte buffer here! ***");
	}

	@Override
	public void handleBuffer(Nio nio, String refToBuffer) {
		// TODO Auto-generated method stub
		System.out.println("buffer location = "+refToBuffer);
	}

}
