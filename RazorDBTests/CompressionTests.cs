using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.IO;
using System.Diagnostics;
using RazorDB;

namespace RazorDBTests {

    [TestFixture]
    public class CompressionTests {

        [Test]
        public void SnapCompMemorySpeed() {

            byte[] buff = new byte[32 * 1024];
            byte[] outBuff = new byte[64 * 1024];
            string infile = @"RazorDB.pdb";

            int inputBytes = 0;
            int outputBytes = 0;

            using (var ifile = File.Open(infile, FileMode.Open, FileAccess.Read)) {
                ifile.Read(buff, 0, buff.Length);
                ifile.Close();
            }

            Stopwatch timer = new Stopwatch();
            timer.Start();
            for (int i = 0; i < 10000; i++) {
                inputBytes += buff.Length;
                int outSize = Compression.Compress(buff, buff.Length, outBuff);
                outputBytes += outSize;
            }
            timer.Stop();
            Console.WriteLine("Compression {0}% {1}ms Throughput: {2} MB/s Size: {3} <= {4}", (double)outputBytes / (double)inputBytes, timer.ElapsedMilliseconds, inputBytes / 1024 / 1024 / timer.Elapsed.TotalSeconds, outputBytes, inputBytes);

        }

        [Test]
        public void SnapDecompMemorySpeed() {

            byte[] buff = new byte[32 * 1024];
            byte[] outBuff = new byte[32 * 1024];
            byte[] compBuff = new byte[32 * 1024];
            string infile = @"RazorDB.pdb";

            int inputBytes = 0;
            int outputBytes = 0;

            using (var ifile = File.Open(infile, FileMode.Open, FileAccess.Read)) {
                ifile.Read(buff, 0, buff.Length);
                ifile.Close();
            }
            int outSize = Compression.Compress(buff, buff.Length, outBuff);

            Stopwatch timer = new Stopwatch();
            timer.Start();
            for (int i = 0; i < 10000; i++) {
                inputBytes += outSize;
                int decompSize = Compression.Decompress(outBuff, 0, outSize, compBuff, 0);
                outputBytes += decompSize;
            }
            timer.Stop();
            Console.WriteLine("Decompression {0}% {1}ms Throughput: {2} MB/s Size: {3} <= {4}", (double)outputBytes / (double)inputBytes, timer.ElapsedMilliseconds, outputBytes / 1024 / 1024 / timer.Elapsed.TotalSeconds, outputBytes, inputBytes);

            for (int i = 0; i < buff.Length; i++) {
                Assert.AreEqual(buff[i], compBuff[i]);
            }
        }

        [Test]
        public void SnapParallelCompMemorySpeed() {

            byte[] buff = new byte[32 * 1024];
            byte[] outBuffA = new byte[32 * 1024];
            byte[] outBuffB = new byte[32 * 1024];
            string infile = @"RazorDB.pdb";

            int inputBytes = 0;
            int outputBytes = 0;

            using (var ifile = File.Open(infile, FileMode.Open, FileAccess.Read)) {
                ifile.Read(buff, 0, buff.Length);
                ifile.Close();
            }

            Stopwatch timer = new Stopwatch();
            timer.Start();
            for (int i = 0; i < 1000; i++) {
                inputBytes += buff.Length * 2;
                PairInt outSize = Compression.ParallelCompress(buff, buff.Length, buff, buff.Length, outBuffA, outBuffB);
                outputBytes += outSize.lengthA + outSize.lengthB;
            }
            timer.Stop();
            Console.WriteLine("Compression {0}% {1}ms Throughput: {2} MB/s Size: {3} <= {4}", (double)outputBytes / (double)inputBytes, timer.ElapsedMilliseconds, inputBytes / 1024 / 1024 / timer.Elapsed.TotalSeconds, outputBytes, inputBytes);

        }

        [Test]
        public void SnapParallelDecompMemorySpeed() {

            byte[] buff = new byte[32 * 1024];
            byte[] outBuff = new byte[32 * 1024];
            byte[] compBuffA = new byte[32 * 1024];
            byte[] compBuffB = new byte[32 * 1024];
            string infile = @"RazorDB.pdb";

            int inputBytes = 0;
            int outputBytes = 0;

            using (var ifile = File.Open(infile, FileMode.Open, FileAccess.Read)) {
                ifile.Read(buff, 0, buff.Length);
                ifile.Close();
            }
            int outSize = Compression.Compress(buff, buff.Length, outBuff);

            Stopwatch timer = new Stopwatch();
            timer.Start();
            for (int i = 0; i < 10000; i++) {
                inputBytes += outSize;
                PairInt decompSize = Compression.ParallelDecompress(outBuff, 0, outSize, outBuff, 0, outSize, compBuffA, 0, compBuffB, 0);
                outputBytes += decompSize.lengthA + decompSize.lengthB;
            }
            timer.Stop();
            Console.WriteLine("Decompression {0}% {1}ms Throughput: {2} MB/s Size: {3} <= {4}", (double)outputBytes / (double)inputBytes, timer.ElapsedMilliseconds, outputBytes / 1024 / 1024 / timer.Elapsed.TotalSeconds, outputBytes, inputBytes);

            for (int i = 0; i < buff.Length; i++) {
                Assert.AreEqual(buff[i], compBuffA[i]);
            }
            for (int i = 0; i < buff.Length; i++) {
                Assert.AreEqual(buff[i], compBuffB[i]);
            }
        }
    }
}
