using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace RazorDB {

    public static class Config {

        public static int MaxMemTableSize = 1 * 1024 * 1024;    // Maximum size we should let the memtable grow to in memory before compacting.
        public static int SortedBlockSize = 32 * 1024;          // Size of each block in the sorted table files.
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

    }
}
