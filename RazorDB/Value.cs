using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {

    public struct Value {

        public Value(ByteArray bytes) {
            _bytes = bytes;
        }
        public Value(byte[] bytes) {
            _bytes = new ByteArray(bytes);
        }
        private ByteArray _bytes;

        public byte[] ValueBytes {
            get { return _bytes.InternalBytes; }
        }
        public byte[] InternalBytes {
            get { return _bytes.InternalBytes; }
        }
        public int Length { get { return _bytes.Length; } }

        public static Value Random(int numBytes) {
            return new Value(ByteArray.Random(numBytes));
        }

        public override string ToString() {
            return _bytes.InternalBytes.ToHexString();
        }

        public static Value FromBytes(byte[] bytes) {
            return new Value(new ByteArray(bytes));
        }

        public static Value Empty {
            get { return new Value(new byte[0]); }
        }
    }
}
