using System;
using NUnit.Framework;
using System.Text;
using RazorDB;
using System.IO;
using System.Diagnostics;
using System.Threading;
using System.Collections.Generic;

namespace RazorDBTests {

    [TestFixture]
    public class GetSetTests {

        [TestFixtureSetUp]
        public void Setup() {
            string path = Path.GetFullPath("TestData");
            if (!Directory.Exists(path)) 
                Directory.CreateDirectory(path);
        }

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

                db.Manifest.Logger = (msg) => { Console.WriteLine(msg); };

                timer.Start();
                for (int i = 0; i < 105000; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    totalSize += randomKey.Length + randomValue.Length;
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }

            Manifest mf = new Manifest(path);
            mf.Logger = (msg) => { Console.WriteLine(msg); };
            mf.LogContents();
       }

        [Test]
        public void BulkThreadedSet() {

            int numThreads = 10;
            int totalItems = 100000;
            int totalSize = 0;

            string path = Path.GetFullPath("TestData\\BulkThreadedSet");

            List<Thread> threads = new List<Thread>();
            using (var db = new KeyValueStore(path)) {

                for (int j = 0; j < numThreads; j++) {
                    threads.Add(new Thread( (num) => {
                        int itemsPerThread = totalItems / numThreads;
                        for (int i = 0; i < itemsPerThread; i++) {
                            var randomKey = new ByteArray( BitConverter.GetBytes( ((int)num * itemsPerThread) + i ) );
                            var randomValue = ByteArray.Random(256);
                            db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                            Interlocked.Add(ref totalSize, randomKey.Length + randomValue.Length);
                        }
                    }));
                }

                var timer = new Stopwatch();
                timer.Start();

                // Start all the threads
                int tnum = 0;
                threads.ForEach((t) => t.Start(tnum++));

                // Wait on all the threads to complete
                threads.ForEach((t) => t.Join(300000));

                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }

        }

        [Test]
        public void BulkSetBulkGet() {

            string path = Path.GetFullPath("TestData\\BulkSetBulkGet");
            var timer = new Stopwatch();
            int totalSize = 0;

            var items = new Dictionary<ByteArray, ByteArray>();

            using (var db = new KeyValueStore(path)) {

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < 105000; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    if (i % 100 == 0) {
                        items[randomKey] = randomValue;
                        totalSize += randomKey.Length + randomValue.Length;
                    }
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
            }
            using (var db = new KeyValueStore(path)) {
                
                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Reset();
                timer.Start();
                foreach ( var insertedItem in items) {
                    try {
                        byte[] value = db.Get(insertedItem.Key.InternalBytes);
                        Assert.AreEqual(insertedItem.Value, new ByteArray(value));
                    } catch (Exception e) {
                        Console.WriteLine("Key: {0}\n{1}",insertedItem.Key,e);
                        Debugger.Launch();
                        db.Get(insertedItem.Key.InternalBytes);
                        db.Manifest.LogContents();
                        throw;
                    }
                }
                timer.Stop();
                Console.WriteLine("Randomized read throughput of {0} MB/s (avg {1} ms per lookup)", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)items.Count);

            }

        }

        [Test]
        public void BulkSetGetWhileReMerging() {

            string path = Path.GetFullPath("TestData\\BulkSetGetWhileReMerging");
            var timer = new Stopwatch();
            int totalSize = 0;

            var items = new Dictionary<ByteArray, ByteArray>();

            using (var db = new KeyValueStore(path)) {

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                timer.Start();
                for (int i = 0; i < 105000; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);

                    if (i % 100 == 0) {
                        items[randomKey] = randomValue;
                        totalSize += randomKey.Length + randomValue.Length;
                    }
                }
                timer.Stop();
                Console.WriteLine("Wrote sorted table at a throughput of {0} MB/s", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));

                timer.Reset();
                timer.Start();
                foreach (var insertedItem in items) {
                    try {
                        byte[] value = db.Get(insertedItem.Key.InternalBytes);
                        Assert.AreEqual(insertedItem.Value, new ByteArray(value));
                    } catch (Exception e) {
                        Console.WriteLine("Key: {0}\n{1}", insertedItem.Key, e);
                        Debugger.Launch();
                        db.Get(insertedItem.Key.InternalBytes);
                        db.Manifest.LogContents();
                        throw;
                    }
                }
                timer.Stop();
                Console.WriteLine("Randomized read throughput of {0} MB/s (avg {1} ms per lookup)", (double)totalSize / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0), (double)timer.Elapsed.TotalSeconds / (double)items.Count);

            }

        }

    }

}