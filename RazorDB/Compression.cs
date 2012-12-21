using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

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
                outputLengthA = Compress(inputBufferA, lengthA, outputBufferA);
                evtA.Set();
             });
            ThreadPool.QueueUserWorkItem((object state) => {
                outputLengthB = Compress(inputBufferB, lengthB, outputBufferB);
                evtB.Set();
            });
            WaitHandle.WaitAll(new WaitHandle[] { evtA, evtB });
            return new PairInt(outputLengthA, outputLengthB);
        }

        static public int Compress(byte[] inputBuffer, int length, byte[] outputBuffer) {

            byte[] target = outputBuffer;
            int targetIndex = 0;
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

            int[] ilhm = new int[length / 5];
            for (int k = 0; k < ilhm.Length; k++) {
                ilhm[k] = -1;
            }

            for (int i = offset; i + 4 < length && i < offset + 4; i++) {
                ilhm[toInt(inputBuffer, i) % ilhm.Length] = i;
            }

            for (int i = offset + 4; i < offset + length; i++) {
                Hit h = search(inputBuffer, i, length, ilhm);
                if (i + 4 < offset + length) {
                    ilhm[toInt(inputBuffer, i) % ilhm.Length] = i;
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
                        Array.Copy(inputBuffer, lasthit, target, targetIndex, i - lasthit);
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
                        if (i + 4 < inputBuffer.Length) {
                            ilhm[toInt(inputBuffer, i) % ilhm.Length] = i;
                        }
                    }
                    lasthit = i + h.length;
                    while (i < lasthit - 1) {
                        if (i + 4 < inputBuffer.Length) {
                            ilhm[toInt(inputBuffer, i) % ilhm.Length] = i;
                        }
                        i++;
                    }
                } else {
                    if (i + 4 < length) {
                        ilhm[toInt(inputBuffer, i) % ilhm.Length] = i;
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
                Array.Copy(inputBuffer, lasthit, target, targetIndex, length - lasthit);
                targetIndex += length - lasthit;
            }

            return targetIndex;
        }

        private static Hit search(byte[] source, int index, int length, int[] ilhm) {

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

            int fp = ilhm[toInt(source, index) % ilhm.Length];
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

        private static int toInt(byte[] data, int offset) {
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

        public static int Decompress(byte[] inputBuffer, int inputOffset, int inputCount, byte[] outputBuffer, int outputOffset) {

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
                        Array.Copy(inputBuffer, sourceIndex, outputBuffer, targetIndex, l);
                        sourceIndex += l;
                        targetIndex += l;
                        break;
                    case 1:
                        l = 4 + ((inputBuffer[sourceIndex] >> 2) & 7);
                        o = (inputBuffer[sourceIndex++] & 0xe0) << 3;
                        o |= inputBuffer[sourceIndex++] & 0xff;
                        if (l < o) {
                            Array.Copy(outputBuffer, targetIndex - o, outputBuffer, targetIndex, l);
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
                                    Array.Copy(outputBuffer, targetIndex - o, outputBuffer, targetIndex, c);
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
                            Array.Copy(outputBuffer, targetIndex - o, outputBuffer, targetIndex, l);
                            targetIndex += l;
                        } else {
                            while (l > 0) {
                                c = l > o ? o : l;
                                Array.Copy(outputBuffer, targetIndex - o, outputBuffer, targetIndex, c);
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
                            Array.Copy(outputBuffer, targetIndex - o, outputBuffer, targetIndex, l);
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
                                    Array.Copy(outputBuffer, targetIndex - o, outputBuffer, targetIndex, c);
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
