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

        public int CompareTo(ByteArray other) {
            return CompareMemCmp(_bytes, other._bytes);
        }

        public override bool Equals(object obj) {
            if (obj != null && obj is ByteArray) {
                return CompareMemCmp(_bytes, ((ByteArray)obj)._bytes) == 0;
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
            return memcmp(left, right, Math.Min(l, r));
        }

        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int memcmp(byte[] arr1, byte[] arr2, int cnt);
    }

}
