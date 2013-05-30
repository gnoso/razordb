/*
Copyright 2012, 2013 Gnoso Inc.

This software is licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except for what is in compliance with the License.

You may obtain a copy of this license at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.

See the License for the specific language governing permissions and limitations.
*/
using System;

namespace RazorDB {

	// For use to help w/ byte and hexadecimal conversions (pieces exerpted from jburman's ZeroG)
	public class BinaryHelper {

		public static string ByteToHexString(byte[] bytes) {
			char[] c = new char[bytes.Length * 2];
			byte b;

			for (int i = 0; i < bytes.Length; i++) {
				b = ((byte)(bytes[i] >> 4));
				c[i * 2] = (char)(b > 9 ? b + 0x37 : b + 0x30);
				b = ((byte)(bytes[i] & 0xF));
				c[i * 2 + 1] = (char)(b > 9 ? b + 0x37 : b + 0x30);
			}

			return new string(c);
		}

		public static byte[] HexStringToByte(string hex) {
			int len = hex.Length;
			byte[] bytes = new byte[len / 2];

			for (int i = 0; len > i; i += 2) {
				bytes[i / 2] = Convert.ToByte(hex.Substring(i, 2), 16);
			}

			return bytes;
		}
	}
}