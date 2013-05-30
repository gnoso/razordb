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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;

namespace RazorDB {

    public struct ByteArray : IComparable<ByteArray> {

        public ByteArray(byte[] bytes) {
            if (bytes == null)
                throw new ArgumentNullException();
            _bytes = bytes;
        }
        private byte[] _bytes;

        public byte[] InternalBytes { get { return _bytes; } }

        public int Length {
            get { return _bytes == null ? 0 : _bytes.Length; }
        }

        public int CompareTo(ByteArray other) {
            if (_bytes == null && other._bytes != null) {
                return -1;
            } else if (_bytes != null && other._bytes == null) {
                return 1;
            } else if (_bytes == null && other._bytes == null) {
                return 0;
            }
            return CompareMemCmp(_bytes, other._bytes);
        }

        public int CompareTo(byte[] other, int offset, int length) {
            return CompareMemCmp(_bytes, 0, other, offset, Math.Min(_bytes.Length, length));
        }

        public static bool operator ==(ByteArray a, ByteArray b) {
            return CompareMemCmp(a._bytes, b._bytes) == 0;
        }

        public static bool operator !=(ByteArray a, ByteArray b) {
            return CompareMemCmp(a._bytes, b._bytes) != 0;
        }

        [ThreadStatic]
        private static Random rand;
        private static object randLock = new object();
        private static Random getRand() {
            if (rand == null)
                lock (randLock) {
                    if (rand == null)
                        rand = new Random();
                }
            return rand;
        }

        public static ByteArray Random(int numBytes) {
            byte[] bytes = new byte[numBytes];
            getRand().NextBytes(bytes);
            return new ByteArray(bytes);
        }

        public override bool Equals(object obj) {
            if (obj != null && obj is ByteArray) {
                return this == (ByteArray)obj;
            } else {
                return false;
            }
        }

        public static ByteArray Empty {
            get { return new ByteArray(new byte[0]); }
        }

        public override int GetHashCode() {
            return (int)MurmurHash2Unsafe.Default.Hash(_bytes);
        }

        public static int CompareMemCmp(byte[] left, byte[] right) {
            int l = left.Length;
            int r = right.Length;
            int comparison = CompareMemCmp(left, 0, right, 0, Math.Min(l, r));
            if (comparison == 0 && l != r) {
                return l.CompareTo(r);
            } else {
                return comparison;
            }
        }

        public static unsafe int CompareMemCmp(byte[] buffer1, int offset1, byte[] buffer2, int offset2, int count) {
            fixed (byte* b1 = buffer1, b2 = buffer2) {
                return memcmp(b1 + offset1, b2 + offset2, count);
            }
        }

        public override string ToString() {
            return _bytes.ToHexString();
        }

        [DllImport("msvcrt.dll")]
        private static extern unsafe int memcmp(byte* b1, byte* b2, int count);

        public static ByteArray From(byte[] block, int offset, int size) {
            byte[] bytes = new byte[size];
            Array.Copy(block, offset, bytes, 0, size);
            return new ByteArray(bytes);
        }
    }

    public static class ByteArrayHelper {
        public static string ToHexString(this byte[] bytes) {
            return string.Concat(bytes.Select((b) => b.ToString("X2")).ToArray());
        }
        public static string ToHexString(this byte[] bytes, int offset, int count) {
            return string.Concat(bytes.Skip(offset).Take(count).Select((b) => b.ToString("X2")).ToArray());
        }
    }

}
