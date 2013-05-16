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

namespace RazorDB {

    public enum ValueFlag : byte { Null = 0x0, SmallValue = 0x1, LargeValueDescriptor = 0x5, LargeValueChunk = 0x10, Deleted = 0xFF };

    public struct Value {

        public Value(byte[] bytes) : this(bytes, ValueFlag.SmallValue) {}
        public Value(byte[] bytes, ValueFlag type) {
            byte[] b = new byte[bytes.Length + 1];
            b[0] = (byte) type;
            Array.Copy(bytes, 0, b, 1, bytes.Length);
            _bytes = new ByteArray(b);
        }
        private ByteArray _bytes;

        public ValueFlag Type {
            get { return (ValueFlag) _bytes.InternalBytes[0]; }
        }

        public byte[] ValueBytes {
            get {
                byte[] v = new byte[Length - 1];
                Array.Copy(InternalBytes, 1, v, 0, Length - 1);
                return v; 
            }
        }
        public int CopyValueBytesTo(byte[] block, int offset) {
            Array.Copy(InternalBytes, 1, block, offset, Length - 1);
            return Length - 1;
        }
        public byte[] InternalBytes {
            get { return _bytes.InternalBytes; }
        }
        public int Length { get { return _bytes.Length; } }

        public static Value Random(int numBytes) {
            return Value.FromBytes(ByteArray.Random(numBytes).InternalBytes);
        }

        public override string ToString() {
            return _bytes.InternalBytes.ToHexString();
        }

        public static Value FromBytes(byte[] bytes) {
            return From(bytes, 0, bytes.Length);
        }

        public static Value From(byte[] bytes, int offset, int length) {
            if (length <= 0)
                throw new ArgumentOutOfRangeException("Length of the Value must be at least 1 byte.");

            var v = new Value();
            v._bytes = ByteArray.From(bytes, offset, length);
            return v;
        }

        public static Value Empty {
            get { return new Value(new byte[0], ValueFlag.Null); }
        }

        public static Value Deleted {
            get { return new Value(new byte[0], ValueFlag.Deleted); }
        }
    }


}
