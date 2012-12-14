﻿/* 
Copyright 2012 Gnoso Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace RazorDB {

    public static class Config {

        public static int IndexCacheSize = 10 * 1024 * 1024;                    // Size of the Block Index Cache in bytes
        public static int DataBlockCacheSize = 100 * 1024 * 1024;               // Size of the Data Block Cache in bytes
        public static int MaxSortedBlockTableSize = 2 * 1024 * 1024;            // Maximum size we should let the sorted block table grow to before rolling over to a new file.
        public static int MaxMemTableSize = 1 * 1024 * 1024;                    // Maximum size we should let the memtable grow to in memory before compacting.
        public static int SortedBlockSize = 32 * 1024;                          // Size of each block in the sorted table files (Warning: changing this number will break future readers).
        public static int ManifestVersionCount = 1000;                          // Number of manifests to append before rolling the file over
        public static int MaxSmallValueSize = SortedBlockSize / 4;              // The maximum size of the value that we store contiguously. Anything larger than this is split into multiple parts.
        public static int MaxLargeValueSize = MaxSmallValueSize * (0xFF - 1);   // The largest size of the value that we can store (in multiple parts) using the current configuration.

        public static string SortedBlockTableFile(string baseName, int level, int version) {
            return baseName + "\\" + level.ToString() + "-" + version.ToString() + ".sbt";
        }
        public static string JournalFile(string baseName, int version) {
            return baseName + "\\" + version.ToString() + ".jf";
        }
        public static string ManifestFile(string baseName) {
            return baseName + "\\0.mf";
        }
        public static string IndexBaseName(string baseName, string indexName) {
            return baseName + "\\" + indexName;
        }
        public static int MaxPagesOnLevel(int level) {
            if (level == 0) {
                return 4;
            } else {
                return (int) Math.Pow(10, level);
            }
        }
    }

    public static class Helper {

        public static int Encode7BitInt(byte[] workingArray, int value) {
            int size = 0;
            uint num = (uint)value;
            while (num >= 0x80) {
                workingArray[size] = (byte)(num | 0x80);
                size++;
                num = num >> 7;
            }
            workingArray[size] = (byte)num;
            size++;
            return size;
        }

        public static int Decode7BitInt(byte[] workingArray, ref int offset) {
            byte b;
            int val = 0;
            int bits = 0;
            do {
                b = workingArray[offset];
                offset++;
                val |= (b & 0x7f) << bits;
                bits += 7;
            }
            while ((b & 0x80) != 0);
            return val;
        }
        public static int Read7BitEncodedInt(this BinaryReader rdr) {
            return (int) rdr.Read7BitEncodedUInt();
        }

        public static uint Read7BitEncodedUInt(this BinaryReader rdr) {
            byte b;
            int val = 0;
            int bits = 0;
            do {
                b = rdr.ReadByte();
                val |= (b & 0x7f) << bits;
                bits += 7;
            }
            while ((b & 0x80) != 0);
            return (uint)val;
        }

        public static void Write7BitEncodedInt(this BinaryWriter wtr, int value) {
            if (value < 0)
                throw new InvalidDataException("Negative numbers are not supported.");
            wtr.Write7BitEncodedUInt( (uint) value);
        }
        public static void Write7BitEncodedUInt(this BinaryWriter wtr, uint value) {
            uint num = value;
            while (num >= 0x80) {
                wtr.Write((byte)(num | 0x80));
                num = num >> 7;
            }
            wtr.Write((byte)num);
        }

    }
}
