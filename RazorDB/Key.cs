/*
Copyright 2012-2015 Gnoso Inc.

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

namespace RazorDB {

    public struct Key : IComparable<Key>, IEquatable<Key> {

        public Key(ByteArray bytes) {
            _bytes = bytes;
        }
        public Key(byte[] bytes, byte seqNum) {
            byte[] internalBytes = new byte[bytes.Length + 1];
            Helper.BlockCopy(bytes, 0, internalBytes, 0, bytes.Length);
            internalBytes[bytes.Length] = seqNum;
            _bytes = new ByteArray(internalBytes);
        }
        private ByteArray _bytes;

        public byte[] KeyBytes {
            get { 
                byte[] keyBytes = new byte[Length-1];
                Helper.BlockCopy(InternalBytes, 0, keyBytes, 0, Length-1);
                return keyBytes;
            }
        }
        public byte[] InternalBytes { 
            get { return _bytes.InternalBytes; } 
        }
        public int Length { get { return _bytes.Length; } }

        public byte SequenceNum { get { return _bytes.InternalBytes[Length-1]; } }

        public static Key Random(int numBytes) {
            return new Key(ByteArray.Random(numBytes));
        }
        public int CompareTo(Key other) {
            return _bytes.CompareTo(other._bytes);
        }
        public int CompareTo(byte[] other, int offset, int length) {
            return _bytes.CompareTo(other, offset, length);
        }
        public override bool Equals(object obj) {
            return obj is Key ? base.Equals( (Key) obj) : false;
        }

        public override int GetHashCode() {
            return _bytes.GetHashCode();
        }

        public override string ToString() {
            return _bytes.InternalBytes.ToHexString(0, Length-1) + ":" + SequenceNum.ToString();
        }

        public bool Equals(Key other) {
            return this.CompareTo(other) == 0;
        }

        public static Key FromBytes(byte[] bytes) {
            return new Key(new ByteArray(bytes));
        }

        public static Key Empty {
            get { return new Key(new byte[0], 0); }
        }
        public bool IsEmpty {
            get { return _bytes.Length == 1 && _bytes.InternalBytes[0] == 0; }
        }
        public Key WithSequence(byte seqNum) {
            return new Key(KeyBytes, seqNum);
        }

        static unsafe short LengthOfMatchingPrefix(byte[] a1, byte[] a2) {
            if (a1 == null || a2 == null)
                return (short)0;

            fixed (byte* p1 = a1, p2 = a2) {
                byte* x1 = p1, x2 = p2;
                int l1 = a1.Length;
                int l2 = a2.Length;
                short i = 0;
                for (i = 0; i < l1 && i < l2; i++, x1 += 1, x2 += 1)
                    if (*x1 != *x2) return i;
                return i;
            }

        }

        public short PrefixLength(byte[] matchPrefix) {
            return LengthOfMatchingPrefix(InternalBytes, matchPrefix);
        }

        internal int PrefixCompareTo(byte[] prefixKey, short prefixLen, byte[] block, int offset, int keySize, out byte[] nextKey) {
            nextKey = new byte[prefixLen + keySize];
            Buffer.BlockCopy(prefixKey, 0, nextKey, 0, prefixLen);
            Buffer.BlockCopy(block, offset, nextKey, prefixLen, keySize);
            return CompareTo(nextKey, 0, nextKey.Length);
        }

        internal static Key KeyFromPrefix(byte[] prefixKey, short prefixLen, byte[] block, int offset, int keySize) {
            var mergeKey = new byte[prefixLen + keySize];
            Buffer.BlockCopy(prefixKey, 0, mergeKey, 0, prefixLen);
            Buffer.BlockCopy(block, offset, mergeKey, prefixLen, keySize);
            return new Key(new ByteArray(mergeKey));
        }
    }
}
