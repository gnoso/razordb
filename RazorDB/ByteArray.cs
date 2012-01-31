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
            get { return _bytes.Length; }
        }

        public int CompareTo(ByteArray other) {
            return CompareMemCmp(_bytes, other._bytes);
        }

        public static bool operator ==(ByteArray a, ByteArray b) {
            return CompareMemCmp(a._bytes, b._bytes) == 0;
        }

        public static bool operator !=(ByteArray a, ByteArray b) {
            return CompareMemCmp(a._bytes, b._bytes) != 0;
        }

        private static Random rand = new Random();
        public static ByteArray Random(int numBytes) {
            byte[] bytes = new byte[numBytes];
            rand.NextBytes(bytes);
            return new ByteArray(bytes);
        }

        public override bool Equals(object obj) {
            if (obj != null && obj is ByteArray) {
                return this == (ByteArray)obj;
            } else {
                return false;
            }
        }

        public override int GetHashCode() {
            return (int)MurmurHash2Unsafe.Default.Hash(_bytes);
        }

        public static int CompareMemCmp(byte[] left, byte[] right) {
            int l = left.Length;
            int r = right.Length;
            int comparison = memcmp(left, right, Math.Min(l, r));
            if (comparison == 0 && l != r) {
                return l.CompareTo(r);
            } else {
                return comparison;
            }
        }

        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int memcmp(byte[] arr1, byte[] arr2, int cnt);

        public static ByteArray From(byte[] block, int offset, int size) {
            byte[] bytes = new byte[size];
            Array.Copy(block, offset, bytes, 0, size);
            return new ByteArray(bytes);
        }
    }

}
