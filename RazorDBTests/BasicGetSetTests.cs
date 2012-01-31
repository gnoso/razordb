using System;
using NUnit.Framework;
using System.Text;
using RazorDB;
using System.IO;
using System.Diagnostics;

namespace RazorDBTests {

    [TestFixture]
    public class BasicGetSetTests {

        [Test]
        public void BasicGetAndSet() {

            using (var db = new KeyValueStore("TestData\\GetAndSet")) {

                for (int i = 0; i < 10; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                    db.Set(key, value);
                }

                for (int j = 0; j < 15; j++) {
                    byte[] key = BitConverter.GetBytes(j);

                    byte[] value = db.Get(key);
                    if (j < 10) {
                        Assert.AreEqual(Encoding.UTF8.GetBytes("Number " + j.ToString()), value);
                    } else {
                        Assert.IsNull(value);
                    }
                }
            }
        }

        [Test]
        public void BasicPersistentGetAndSet() {

            string path = Path.GetFullPath("TestData\\GetAndSet");
            using (var db = new KeyValueStore(path)) {

                for (int i = 0; i < 10; i++) {
                    byte[] key = BitConverter.GetBytes(i);
                    byte[] value = Encoding.UTF8.GetBytes("Number " + i.ToString());
                    db.Set(key, value);
                }
            }

            using (var db = new KeyValueStore(path)) {
                for (int j = 0; j < 15; j++) {
                    byte[] key = BitConverter.GetBytes(j);

                    byte[] value = db.Get(key);
                    if (j < 10) {
                        Assert.AreEqual(Encoding.UTF8.GetBytes("Number " + j.ToString()), value);
                    } else {
                        Assert.IsNull(value);
                    }
                }
            }
        }

        [Test]
        public void BulkSet() {

            string path = Path.GetFullPath("TestData\\BulkSet");
            var timer = new Stopwatch();
            int totalSize = 0;

            using (var db = new KeyValueStore(path)) {

                timer.Start();
                for (int i = 0; i < 500000; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
            }

            Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
        }


    }

}