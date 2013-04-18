using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RazorDB {
    public struct Key : IComparable<Key>, IEquatable<Key> {
        public Key(ByteArray bytes) {
            _bytes = bytes;
        }
		public static Key Random(int numBytes) {
			return new Key(ByteArray.Random(numBytes));
		}
        public Key(byte[] bytes, byte seqNum) {
            byte[] internalBytes = new byte[bytes.Length + 1];
            Array.Copy(bytes, internalBytes, bytes.Length);
            internalBytes[bytes.Length] = seqNum;
            _bytes = new ByteArray(internalBytes);
        }

        ByteArray _bytes;
        public byte[] KeyBytes {
            get { 
                byte[] keyBytes = new byte[Length-1];
                Array.Copy(InternalBytes, keyBytes, Length-1);
                return keyBytes;
            }
        }
        public byte[] InternalBytes { 
            get {
				return _bytes.InternalBytes;
			}
        }
		public byte SequenceNum {
			get {
				return _bytes.InternalBytes[Length-1];
			}
		}

        public int Length {
			get {
				return _bytes.Length;
			}
		}

        public int CompareTo(Key other) {
            return _bytes.CompareTo(other._bytes);
        }
        public int CompareTo(byte[] other, int offset, int length) {
            return _bytes.CompareTo(other, offset, length);
        }

        public override int GetHashCode() {
            return _bytes.GetHashCode();
        }

        public override string ToString() {
            return _bytes.InternalBytes.ToHexString(0, Length-1) + ":" + SequenceNum.ToString();
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

        public bool IsEmpty {
            get {
				return _bytes.Length == 1 && _bytes.InternalBytes[0] == 0;
			}
        }
		public bool Equals(Key other) {
			return this.CompareTo(other) == 0;
		}
		public override bool Equals(object obj) {
			return obj is Key ? base.Equals( (Key) obj) : false;
		}
    }
}