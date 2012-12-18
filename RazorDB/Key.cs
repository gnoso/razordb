/* 
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
using System.IO;
using System.Linq;
using System.Text;

namespace RazorDB {

    public struct Key : IComparable<Key>, IEquatable<Key> {

        public Key(ByteArray bytes) {
            _bytes = bytes;
        }
        public Key(byte[] bytes, byte seqNum) {
            byte[] internalBytes = new byte[bytes.Length + 1];
            Array.Copy(bytes, internalBytes, bytes.Length);
            internalBytes[bytes.Length] = seqNum;
            _bytes = new ByteArray(internalBytes);
        }
        private ByteArray _bytes;

        public byte[] KeyBytes {
            get { 
                byte[] keyBytes = new byte[Length-1];
                Array.Copy(InternalBytes, keyBytes, Length-1);
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
        public Key WithSequence(byte seqNum) {
            return new Key(KeyBytes, seqNum);
        }

    }

    // Key with extended seqnum (uses two bytes instead of 1)
    public struct KeyEx : IComparable<KeyEx>, IEquatable<KeyEx> {

        public KeyEx(ByteArray bytes) {
            _bytes = bytes;
        }

        public KeyEx(byte[] bytes, int seqNum) {
            // Don't allow sequence numbers higher than 32767 because we want to reserve the high bit for future signalling
            if (seqNum > 0x7FFF) {
                throw new ArgumentOutOfRangeException(string.Format("Sequence Number {0} is out of range.", seqNum));
            }

            byte[] internalBytes = new byte[bytes.Length + 2];
            Array.Copy(bytes, internalBytes, bytes.Length);
            internalBytes[bytes.Length] = (byte)(seqNum >> 8);
            internalBytes[bytes.Length + 1] = (byte)(seqNum & 0xFF);
            _bytes = new ByteArray(internalBytes);
        }
        private ByteArray _bytes;

        public byte[] KeyBytes {
            get {
                byte[] keyBytes = new byte[Length - 2];
                Array.Copy(InternalBytes, keyBytes, Length - 2);
                return keyBytes;
            }
        }
        public byte[] InternalBytes {
            get { return _bytes.InternalBytes; }
        }
        public int Length { get { return _bytes.Length; } }

        public int SequenceNum { get { return (_bytes.InternalBytes[Length - 2] << 8) | _bytes.InternalBytes[Length - 1]; } }

        public static KeyEx Random(int numBytes) {
            return new KeyEx(ByteArray.Random(numBytes));
        }
        public int CompareTo(KeyEx other) {
            return _bytes.CompareTo(other._bytes);
        }
        public int CompareTo(byte[] other, int offset, int length) {
            return _bytes.CompareTo(other, offset, length);
        }
        public int CompareToV1(byte[] other, int offset, int length) {
            // Compare the bytes without the sequence number
            int res = _bytes.CompareTo(other, offset, length-1);
            if (res == 0) {
                // Compare the sequence number
                int keyVal = this.SequenceNum;
                int otherVal = other[offset + length - 1];
                res = keyVal.CompareTo(otherVal);
            } 
            return res;
        }
        public override bool Equals(object obj) {
            return obj is KeyEx ? base.Equals((KeyEx)obj) : false;
        }

        public override int GetHashCode() {
            return _bytes.GetHashCode();
        }

        public override string ToString() {
            return _bytes.InternalBytes.ToHexString(0, Length - 2) + ":" + SequenceNum.ToString();
        }

        public bool Equals(KeyEx other) {
            return this.CompareTo(other) == 0;
        }

        public static KeyEx FromKey(Key k) {
            return new KeyEx(k.KeyBytes, k.SequenceNum);
        }

        public static KeyEx FromBytes(byte[] bytes) {
            return new KeyEx(new ByteArray(bytes));
        }

        public static KeyEx Empty {
            get { return new KeyEx(new byte[0], 0); }
        }
        public KeyEx WithSequence(int seqNum) {
            return new KeyEx(KeyBytes, seqNum);
        }

        public void Write(BinaryWriter writer) {
            writer.Write((byte)0xF0);
            writer.Write7BitEncodedInt(Length);
            writer.Write(InternalBytes);
        }

        public static KeyEx FromReader(BinaryReader reader) {
            byte keyVersionByte = reader.ReadByte();
            if (keyVersionByte == 0xF0) {
                int keyLen = reader.Read7BitEncodedInt();
                return KeyEx.FromBytes(reader.ReadBytes(keyLen));
            } else {
                reader.BaseStream.Seek(-1, SeekOrigin.Current); // Move back if the byte wasn't what we were expecting
                int keyLen = reader.Read7BitEncodedInt();
                return KeyEx.FromKey(Key.FromBytes(reader.ReadBytes(keyLen)));
            }
        }

    }

}
