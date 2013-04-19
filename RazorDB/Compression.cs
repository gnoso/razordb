﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Security.Cryptography;
using System.Runtime.InteropServices;

namespace RazorDB {

    public struct PairInt {
        public int lengthA, lengthB;
        public PairInt(int lengthA, int lengthB) {
            this.lengthA = lengthA;
            this.lengthB = lengthB;
        }
    }

    public class Compression {

        static public PairInt ParallelCompress(byte[] inputBufferA, int lengthA, byte[] inputBufferB, int lengthB, byte[] outputBufferA, byte[] outputBufferB) {
            ManualResetEvent evtA = new ManualResetEvent(false);
            ManualResetEvent evtB = new ManualResetEvent(false);
            int outputLengthA = 0, outputLengthB = 0;
            ThreadPool.QueueUserWorkItem((object state) => {
                outputLengthA = Compress(inputBufferA, lengthA, outputBufferA, 0);
                evtA.Set();
            });
            ThreadPool.QueueUserWorkItem((object state) => {
                outputLengthB = Compress(inputBufferB, lengthB, outputBufferB, 0);
                evtB.Set();
            });
            WaitHandle.WaitAll(new WaitHandle[] { evtA, evtB });
            return new PairInt(outputLengthA, outputLengthB);
        }

        [DllImport("msvcrt.dll", SetLastError = false)]
        private static unsafe extern IntPtr memcpy(byte* dest, byte* src, int count);

        static public int Compress(byte[] inpBuffer, int length, byte[] outputBuffer, int outputOffset) {

            unchecked {
                unsafe {
                    fixed (byte* target = outputBuffer)
                    fixed (byte* inputBuffer = inpBuffer) {
                        int targetIndex = outputOffset;
                        int offset = 0;
                        int lasthit = offset;

                        int l = length;
                        while (l > 0) {
                            if (l >= 128) {
                                target[targetIndex++] = (byte)(0x80 | (l & 0x7f));
                            } else {
                                target[targetIndex++] = (byte)l;
                            }
                            l >>= 7;
                        }

                        int ilhmLen = length / 5;
                        fixed (int* ilhm = new int[ilhmLen]) {
                            for (int k = 0; k < ilhmLen; k++) {
                                ilhm[k] = -1;
                            }

                            for (int i = offset; i + 4 < length && i < offset + 4; i++) {
                                ilhm[toInt(inputBuffer, i) % ilhmLen] = i;
                            }

                            for (int i = offset + 4; i < offset + length; i++) {
                                Hit h = search(inputBuffer, i, length, ilhm, ilhmLen);
                                if (i + 4 < offset + length) {
                                    ilhm[toInt(inputBuffer, i) % ilhmLen] = i;
                                }
                                if (h != null) {
                                    if (lasthit < i) {
                                        int len = i - lasthit - 1;
                                        if (len < 60) {
                                            target[targetIndex++] = (byte)(len << 2);
                                        } else if (len < 0x100) {
                                            target[targetIndex++] = (byte)(60 << 2);
                                            target[targetIndex++] = (byte)len;
                                        } else if (len < 0x10000) {
                                            target[targetIndex++] = (byte)(61 << 2);
                                            target[targetIndex++] = (byte)len;
                                            target[targetIndex++] = (byte)(len >> 8);
                                        } else if (len < 0x1000000) {
                                            target[targetIndex++] = (byte)(62 << 2);
                                            target[targetIndex++] = (byte)len;
                                            target[targetIndex++] = (byte)(len >> 8);
                                            target[targetIndex++] = (byte)(len >> 16);
                                        } else {
                                            target[targetIndex++] = (byte)(63 << 2);
                                            target[targetIndex++] = (byte)len;
                                            target[targetIndex++] = (byte)(len >> 8);
                                            target[targetIndex++] = (byte)(len >> 16);
                                            target[targetIndex++] = (byte)(len >> 24);
                                        }
                                        memcpy(target + targetIndex, inputBuffer + lasthit, i - lasthit);
                                        targetIndex += i - lasthit;
                                        lasthit = i;
                                    }
                                    if (h.length <= 11 && h.offset < 2048) {
                                        target[targetIndex] = 1;
                                        target[targetIndex] |= (byte)((h.length - 4) << 2);
                                        target[targetIndex++] |= (byte)((h.offset >> 3) & 0xe0);
                                        target[targetIndex++] = (byte)(h.offset & 0xff);
                                    } else if (h.offset < 65536) {
                                        target[targetIndex] = 2;
                                        target[targetIndex++] |= (byte)((h.length - 1) << 2);
                                        target[targetIndex++] = (byte)(h.offset);
                                        target[targetIndex++] = (byte)(h.offset >> 8);
                                    } else {
                                        target[targetIndex] = 3;
                                        target[targetIndex++] |= (byte)((h.length - 1) << 2);
                                        target[targetIndex++] = (byte)(h.offset);
                                        target[targetIndex++] = (byte)(h.offset >> 8);
                                        target[targetIndex++] = (byte)(h.offset >> 16);
                                        target[targetIndex++] = (byte)(h.offset >> 24);
                                    }
                                    for (; i < lasthit; i++) {
                                        if (i + 4 < inpBuffer.Length) {
                                            ilhm[toInt(inputBuffer, i) % ilhmLen] = i;
                                        }
                                    }
                                    lasthit = i + h.length;
                                    while (i < lasthit - 1) {
                                        if (i + 4 < inpBuffer.Length) {
                                            ilhm[toInt(inputBuffer, i) % ilhmLen] = i;
                                        }
                                        i++;
                                    }
                                } else {
                                    if (i + 4 < length) {
                                        ilhm[toInt(inputBuffer, i) % ilhmLen] = i;
                                    }
                                }
                            }
                        }

                        if (lasthit < offset + length) {
                            int len = (offset + length) - lasthit - 1;
                            if (len < 60) {
                                target[targetIndex++] = (byte)(len << 2);
                            } else if (len < 0x100) {
                                target[targetIndex++] = (byte)(60 << 2);
                                target[targetIndex++] = (byte)len;
                            } else if (len < 0x10000) {
                                target[targetIndex++] = (byte)(61 << 2);
                                target[targetIndex++] = (byte)len;
                                target[targetIndex++] = (byte)(len >> 8);
                            } else if (len < 0x1000000) {
                                target[targetIndex++] = (byte)(62 << 2);
                                target[targetIndex++] = (byte)len;
                                target[targetIndex++] = (byte)(len >> 8);
                                target[targetIndex++] = (byte)(len >> 16);
                            } else {
                                target[targetIndex++] = (byte)(63 << 2);
                                target[targetIndex++] = (byte)len;
                                target[targetIndex++] = (byte)(len >> 8);
                                target[targetIndex++] = (byte)(len >> 16);
                                target[targetIndex++] = (byte)(len >> 24);
                            }
                            memcpy(target + targetIndex, inputBuffer + lasthit, length - lasthit);
                            targetIndex += length - lasthit;
                        }

                        return targetIndex;
                    }
                }
            }
        }

        private unsafe static Hit search(byte* source, int index, int length, int* ilhm, int ilhmLen) {

            if (index + 4 >= length) {
                // We won't search for backward references if there are less than
                // four bytes left to encode, since no relevant compression can be
                // achieved and the map used to store possible back references uses
                // a four byte key.
                return null;
            }

            if (index > 0 &&
                    source[index] == source[index - 1] &&
                    source[index] == source[index + 1] &&
                    source[index] == source[index + 2] &&
                    source[index] == source[index + 3]) {

                // at least five consecutive bytes, so we do
                // run-length-encoding of the last four
                // (three bytes are required for the encoding,
                // so less than four bytes cannot be compressed)

                int len = 0;
                for (int i = index; len < 64 && i < length && source[index] == source[i]; i++, len++) ;
                return new Hit(1, len);
            }

            int fp = ilhm[toInt(source, index) % ilhmLen];
            if (fp < 0) {
                return null;
            }
            int offset = index - fp;
            if (offset < 4) {
                return null;
            }
            int l = 0;
            for (int o = fp, io = index; io < length && source[o] == source[io] && o < index && l < 64; o++, io++) {
                l++;
            }
            return l >= 4 ? new Hit(offset, l) : null;
        }

        private unsafe static int toInt(byte* data, int offset) {
            return
                (((data[offset] & 0xff) << 24) |
                ((data[offset + 1] & 0xff) << 16) |
                ((data[offset + 2] & 0xff) << 8) |
                (data[offset + 3] & 0xff)) & 0x7fffffff;
        }

        private class Hit {
            public int offset, length;

            public Hit(int offset, int length) {
                this.offset = offset;
                this.length = length;
            }
        }

        static public PairInt ParallelDecompress(byte[] inputBufferA, int offsetA, int lengthA, byte[] inputBufferB, int offsetB, int lengthB, byte[] outputBufferA, int outputOffsetA, byte[] outputBufferB, int outputOffsetB) {
            ManualResetEvent evtA = new ManualResetEvent(false);
            ManualResetEvent evtB = new ManualResetEvent(false);
            int outputLengthA = 0, outputLengthB = 0;
            ThreadPool.QueueUserWorkItem((object state) => {
                outputLengthA = Decompress(inputBufferA, offsetA, lengthA, outputBufferA, outputOffsetA);
                evtA.Set();
            });
            ThreadPool.QueueUserWorkItem((object state) => {
                outputLengthB = Decompress(inputBufferB, offsetB, lengthB, outputBufferB, outputOffsetB);
                evtB.Set();
            });
            WaitHandle.WaitAll(new WaitHandle[] { evtA, evtB });
            return new PairInt(outputLengthA, outputLengthB);
        }

        public static int Decompress(byte[] inpBuffer, int inputOffset, int inputCount, byte[] outBuffer, int outputOffset) {

            unchecked {
                unsafe {
                    fixed( byte* inputBuffer = inpBuffer )
                    fixed (byte* outputBuffer = outBuffer) {

                        int i = 0, l, o, c;
                        int sourceIndex = inputOffset, targetIndex = outputOffset;
                        int targetLength = 0;

                        i = 0;
                        do {
                            targetLength += (inputBuffer[sourceIndex] & 0x7f) << (i++ * 7);
                        } while ((inputBuffer[sourceIndex++] & 0x80) == 0x80);

                        while (sourceIndex < inputOffset + inputCount) {

                            if (targetIndex >= targetLength) {
                                throw new FormatException("Superfluous input data encountered on offset " + sourceIndex.ToString());
                            }

                            switch (inputBuffer[sourceIndex] & 3) {
                                case 0:
                                    l = (inputBuffer[sourceIndex++] >> 2) & 0x3f;
                                    switch (l) {
                                        case 60:
                                            l = inputBuffer[sourceIndex++] & 0xff;
                                            l++;
                                            break;
                                        case 61:
                                            l = inputBuffer[sourceIndex++] & 0xff;
                                            l |= (inputBuffer[sourceIndex++] & 0xff) << 8;
                                            l++;
                                            break;
                                        case 62:
                                            l = inputBuffer[sourceIndex++] & 0xff;
                                            l |= (inputBuffer[sourceIndex++] & 0xff) << 8;
                                            l |= (inputBuffer[sourceIndex++] & 0xff) << 16;
                                            l++;
                                            break;
                                        case 63:
                                            l = inputBuffer[sourceIndex++] & 0xff;
                                            l |= (inputBuffer[sourceIndex++] & 0xff) << 8;
                                            l |= (inputBuffer[sourceIndex++] & 0xff) << 16;
                                            l |= (inputBuffer[sourceIndex++] & 0xff) << 24;
                                            l++;
                                            break;
                                        default:
                                            l++;
                                            break;
                                    }
                                    memcpy(outputBuffer + targetIndex, inputBuffer + sourceIndex, l);
                                    sourceIndex += l;
                                    targetIndex += l;
                                    break;
                                case 1:
                                    l = 4 + ((inputBuffer[sourceIndex] >> 2) & 7);
                                    o = (inputBuffer[sourceIndex++] & 0xe0) << 3;
                                    o |= inputBuffer[sourceIndex++] & 0xff;
                                    if (l < o) {
                                        memcpy(outputBuffer + targetIndex, outputBuffer + targetIndex - o, l);
                                        targetIndex += l;
                                    } else {
                                        if (o == 1) {
                                            for (int k = targetIndex; k < targetIndex + l; k++) {
                                                outputBuffer[k] = outputBuffer[targetIndex - 1];
                                            }
                                            targetIndex += l;
                                        } else {
                                            while (l > 0) {
                                                c = l > o ? o : l;
                                                memcpy(outputBuffer + targetIndex, outputBuffer + targetIndex - o, c);
                                                targetIndex += c;
                                                l -= c;
                                            }
                                        }
                                    }
                                    break;
                                case 2:
                                    l = ((inputBuffer[sourceIndex++] >> 2) & 0x3f) + 1;
                                    o = inputBuffer[sourceIndex++] & 0xff;
                                    o |= (inputBuffer[sourceIndex++] & 0xff) << 8;
                                    if (l < o) {
                                        memcpy(outputBuffer + targetIndex, outputBuffer + targetIndex - o, l);
                                        targetIndex += l;
                                    } else {
                                        while (l > 0) {
                                            c = l > o ? o : l;
                                            memcpy(outputBuffer + targetIndex, outputBuffer + targetIndex - o, c);
                                            targetIndex += c;
                                            l -= c;
                                        }
                                    }
                                    break;
                                case 3:
                                    l = ((inputBuffer[sourceIndex++] >> 2) & 0x3f) + 1;
                                    o = inputBuffer[sourceIndex++] & 0xff;
                                    o |= (inputBuffer[sourceIndex++] & 0xff) << 8;
                                    o |= (inputBuffer[sourceIndex++] & 0xff) << 16;
                                    o |= (inputBuffer[sourceIndex++] & 0xff) << 24;
                                    if (l < o) {
                                        memcpy(outputBuffer + targetIndex, outputBuffer + targetIndex - o, l);
                                        targetIndex += l;
                                    } else {
                                        if (o == 1) {
                                            for (int k = targetIndex; k < targetIndex + l; k++) {
                                                outputBuffer[k] = outputBuffer[targetIndex - 1];
                                            }
                                            targetIndex += l;
                                        } else {
                                            while (l > 0) {
                                                c = l > o ? o : l;
                                                memcpy(outputBuffer + targetIndex, outputBuffer + targetIndex - o, c);
                                                targetIndex += c;
                                                l -= c;
                                            }
                                        }
                                    }
                                    break;
                            }
                        }
                        return targetIndex;
                    }
                }
            }
        }

    }

    // CRC32 Algorithm - Credit to Damien Guard
    // Adapted from: http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net
    public class Crc32 : HashAlgorithm {
        public const UInt32 DefaultPolynomial = 0xedb88320;
        public const UInt32 DefaultSeed = 0xffffffff;

        private UInt32 hash;
        private UInt32 seed;
        private UInt32[] table;
        private static UInt32[] defaultTable;

        public Crc32() {
            table = InitializeTable(DefaultPolynomial);
            seed = DefaultSeed;
            Initialize();
        }

        public Crc32(UInt32 polynomial, UInt32 seed) {
            table = InitializeTable(polynomial);
            this.seed = seed;
            Initialize();
        }

        public override void Initialize() {
            hash = seed;
        }

        protected override void HashCore(byte[] buffer, int start, int length) {
            hash = CalculateHash(table, hash, buffer, start, length);
        }

        protected override byte[] HashFinal() {
            byte[] hashBuffer = UInt32ToBigEndianBytes(~hash);
            this.HashValue = hashBuffer;
            return hashBuffer;
        }

        public override int HashSize {
            get { return 32; }
        }

        public static UInt32 Compute(byte[] buffer) {
            return ~CalculateHash(InitializeTable(DefaultPolynomial), DefaultSeed, buffer, 0, buffer.Length);
        }

        public static UInt32 Compute(byte[] buffer, int offset, int length) {
            return ~CalculateHash(InitializeTable(DefaultPolynomial), DefaultSeed, buffer, offset, length);
        }

        public static UInt32 Compute(UInt32 seed, byte[] buffer) {
            return ~CalculateHash(InitializeTable(DefaultPolynomial), seed, buffer, 0, buffer.Length);
        }

        public static UInt32 Compute(UInt32 polynomial, UInt32 seed, byte[] buffer) {
            return ~CalculateHash(InitializeTable(polynomial), seed, buffer, 0, buffer.Length);
        }

        private static UInt32[] InitializeTable(UInt32 polynomial) {
            if (polynomial == DefaultPolynomial && defaultTable != null)
                return defaultTable;

            UInt32[] createTable = new UInt32[256];
            for (int i = 0; i < 256; i++) {
                UInt32 entry = (UInt32)i;
                for (int j = 0; j < 8; j++)
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ polynomial;
                    else
                        entry = entry >> 1;
                createTable[i] = entry;
            }

            if (polynomial == DefaultPolynomial)
                defaultTable = createTable;

            return createTable;
        }

        private static UInt32 CalculateHash(UInt32[] table, UInt32 seed, byte[] buffer, int start, int size) {
            UInt32 crc = seed;
            for (int i = start; i < size; i++)
                unchecked {
                    crc = (crc >> 8) ^ table[buffer[i] ^ crc & 0xff];
                }
            return crc;
        }

        private byte[] UInt32ToBigEndianBytes(UInt32 x) {
            return new byte[] {
			    (byte)((x >> 24) & 0xff),
			    (byte)((x >> 16) & 0xff),
			    (byte)((x >> 8) & 0xff),
			    (byte)(x & 0xff)
		    };
        }
    }
}
