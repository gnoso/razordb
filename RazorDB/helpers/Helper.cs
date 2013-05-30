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
using System.IO;

namespace RazorDB
{
	public static class Helper
	{
		public static int Encode7BitInt (byte[] workingArray, int value)
		{
			int size = 0;
			uint num = (uint)value;
			while (num >= 0x80) {
				workingArray [size] = (byte)(num | 0x80);
				size++;
				num = num >> 7;
			}
			workingArray [size] = (byte)num;
			size++;
			return size;
		}

		public static int Decode7BitInt (byte[] workingArray, ref int offset)
		{
			byte b;
			int val = 0;
			int bits = 0;
			do {
				b = workingArray [offset];
				offset++;
				val |= (b & 0x7f) << bits;
				bits += 7;
			} while ((b & 0x80) != 0);
			return val;
		}

		public static int Read7BitEncodedInt (this BinaryReader rdr)
		{
			return (int)rdr.Read7BitEncodedUInt ();
		}

		public static uint Read7BitEncodedUInt (this BinaryReader rdr)
		{
			byte b;
			int val = 0;
			int bits = 0;
			do {
				b = rdr.ReadByte ();
				val |= (b & 0x7f) << bits;
				bits += 7;
			} while ((b & 0x80) != 0);
			return (uint)val;
		}

		public static void Write7BitEncodedInt (this BinaryWriter wtr, int value)
		{
			if (value < 0)
				throw new InvalidDataException ("Negative numbers are not supported.");
			wtr.Write7BitEncodedUInt ((uint) value);
		}

		public static void Write7BitEncodedUInt (this BinaryWriter wtr, uint value)
		{
			uint num = value;
			while (num >= 0x80) {
				wtr.Write ((byte)(num | 0x80));
				num = num >> 7;
			}
			wtr.Write ((byte)num);
		}
	}
}