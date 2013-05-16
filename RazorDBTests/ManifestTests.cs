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
using NUnit.Framework;
using RazorDB;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace RazorDBTests {

    [TestFixture]
    public class ManifestTests {

        [Test]
        public void WriteAndReadManifest() {

            var path = Path.GetFullPath("TestData\\WriteAndReadManifest");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            // Remove the file if it exists
            var filename = Config.ManifestFile(path);
            if (File.Exists(filename)) 
                File.Delete(filename);

            var mf = new Manifest(path);
            Assert.AreEqual(path, mf.BaseFileName);
            Assert.AreEqual(0, mf.CurrentVersion(0));
            Assert.AreEqual(1, mf.NextVersion(0));
            Assert.AreEqual(1, mf.CurrentVersion(0));
            Assert.AreEqual(1, mf.ManifestVersion);

            var mf2 = new Manifest(path);
            Assert.AreEqual(0, mf2.ManifestVersion);
            Assert.AreEqual(1, mf2.CurrentVersion(0));
            Assert.AreEqual(2, mf2.NextVersion(0));
            Assert.AreEqual(2, mf2.CurrentVersion(0));
            Assert.AreEqual(1, mf2.ManifestVersion);
        }

        [Test]
        public void WriteAndReadManifestMany() {

            var path = Path.GetFullPath("TestData\\WriteAndReadManifestMany");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            // Remove the file if it exists
            var filename = Config.ManifestFile(path);
            if (File.Exists(filename))
                File.Delete(filename);

            var mf = new Manifest(path);
            Assert.AreEqual(0, mf.CurrentVersion(0));

            var timer = new Stopwatch();
            timer.Start();
            for (int i = 0; i < 1100; i++) {
                Assert.AreEqual(i+1, mf.NextVersion(0));
            }
            timer.Stop();

            Console.WriteLine("Committed manifest update in average of {0} ms", (double)timer.ElapsedMilliseconds / 1100.0);

            var mf2 = new Manifest(path);
            Assert.AreEqual(1100, mf2.CurrentVersion(0));
        }

        [Test]
        public void WriteAndReadManifestThreaded() {

            var path = Path.GetFullPath("TestData\\WriteAndReadManifestMulti");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            // Remove the file if it exists
            var filename = Config.ManifestFile(path);
            if (File.Exists(filename))
                File.Delete(filename);

            var mf = new Manifest(path);
            Assert.AreEqual(0, mf.CurrentVersion(0));

            int num_threads = 11;
            List<Thread> threads = new List<Thread>();
            for (int t = 0; t < num_threads; t++) {
                threads.Add(new Thread(() => {
                    for (int i = 0; i < 100; i++) {
                        mf.NextVersion(0);
                    }
                }));
            }

            var timer = new Stopwatch();
            timer.Start();
            threads.ForEach((t) => t.Start());
            threads.ForEach((t) => t.Join());
            timer.Stop();

            Console.WriteLine("Committed manifest update in average of {0} ms", (double)timer.ElapsedMilliseconds / 1100.0);

            var mf2 = new Manifest(path);
            Assert.AreEqual(1100, mf2.CurrentVersion(0));
        }

        [Test]
        public void ManifestAddPages() {

            var path = Path.GetFullPath("TestData\\ManifestAddPages");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            // Remove the file if it exists
            var filename = Config.ManifestFile(path);
            if (File.Exists(filename))
                File.Delete(filename);

            var mf = new Manifest(path);
            mf.AddPage(1, 5, Key.FromBytes(new byte[] { 5 }), Key.FromBytes(new byte[] { 5, 1 }));
            mf.AddPage(1, 6, Key.FromBytes(new byte[] { 6 }), Key.FromBytes(new byte[] { 6, 1 }));
            mf.AddPage(1, 4, Key.FromBytes(new byte[] { 4 }), Key.FromBytes(new byte[] { 4, 1 }));

            using (var mfSnap = mf.GetLatestManifest()) {
                PageRecord[] pg = mfSnap.GetPagesAtLevel(1);
                Assert.AreEqual(1, pg[0].Level);
                Assert.AreEqual(4, pg[0].Version);
                Assert.AreEqual(1, pg[1].Level);
                Assert.AreEqual(5, pg[1].Version);
                Assert.AreEqual(1, pg[2].Level);
                Assert.AreEqual(6, pg[2].Version);
            }

            mf = new Manifest(path);

            mf.ModifyPages(new List<PageRecord>{
                new PageRecord(1, 8, Key.FromBytes( new byte[] { 16 }), Key.FromBytes(new byte[] { 16, 1 }) ),
                new PageRecord(1, 9, Key.FromBytes( new byte[] { 1 }), Key.FromBytes(new byte[] { 1, 1 }) ),
                new PageRecord(1, 16, Key.FromBytes( new byte[] { 10 }), Key.FromBytes(new byte[] { 10, 1 }) )
            }, new List<PageRef>{
                new PageRef{ Level = 1, Version = 6},
                new PageRef{ Level = 1, Version = 4},
            });

            mf = new Manifest(path);

            using (var mfSnap = mf.GetLatestManifest()) {
                var pg = mfSnap.GetPagesAtLevel(1);
                Assert.AreEqual(1, pg[0].Level);
                Assert.AreEqual(9, pg[0].Version);
                Assert.AreEqual(1, pg[1].Level);
                Assert.AreEqual(5, pg[1].Version);
                Assert.AreEqual(1, pg[2].Level);
                Assert.AreEqual(16, pg[2].Version);
                Assert.AreEqual(1, pg[3].Level);
                Assert.AreEqual(8, pg[3].Version);
            }
        }

        [Test]
        public void TestManifestSnapshot() {

            var path = Path.GetFullPath("TestData\\TestManifestSnapshot");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            // Remove the file if it exists
            var filename = Config.ManifestFile(path);
            if (File.Exists(filename))
                File.Delete(filename);

            var mf = new Manifest(path);
            // Add pages and dummy files to represent their contents
            mf.AddPage(1, 5, Key.FromBytes(new byte[] { 5 }), Key.FromBytes(new byte[] { 5, 1 }));
            using (var file = new StreamWriter(Config.SortedBlockTableFile("TestData\\TestManifestSnapshot", 1, 5))) { file.Write("Test"); }
            mf.AddPage(1, 6, Key.FromBytes(new byte[] { 6 }), Key.FromBytes(new byte[] { 6, 1 }));
            using (var file = new StreamWriter(Config.SortedBlockTableFile("TestData\\TestManifestSnapshot", 1, 6))) { file.Write("Test"); }
            mf.AddPage(1, 4, Key.FromBytes(new byte[] { 4 }), Key.FromBytes(new byte[] { 4, 1 }));
            using (var file = new StreamWriter(Config.SortedBlockTableFile("TestData\\TestManifestSnapshot", 1, 4))) { file.Write("Test"); }

            using (var manifestSnapshot = mf.GetLatestManifest()) {

                mf.ModifyPages(new List<PageRecord>{
                    new PageRecord(1, 8, Key.FromBytes( new byte[] { 16 }), Key.FromBytes(new byte[] { 16, 1 }) ),
                    new PageRecord(1, 9, Key.FromBytes( new byte[] { 1 }), Key.FromBytes(new byte[] { 1, 1 }) ),
                    new PageRecord(1, 16, Key.FromBytes( new byte[] { 10 }), Key.FromBytes(new byte[] { 10, 1 }) )
                }, new List<PageRef>{
                    new PageRef{ Level = 1, Version = 6},
                    new PageRef{ Level = 1, Version = 4},
                });

                PageRecord[] pg = manifestSnapshot.GetPagesAtLevel(1);
                Assert.AreEqual(1, pg[0].Level);
                Assert.AreEqual(4, pg[0].Version);
                Assert.AreEqual(1, pg[1].Level);
                Assert.AreEqual(5, pg[1].Version);
                Assert.AreEqual(1, pg[2].Level);
                Assert.AreEqual(6, pg[2].Version);

                // The files should still exist for now
                Assert.IsTrue(File.Exists(Config.SortedBlockTableFile("TestData\\TestManifestSnapshot", 1, 4)));
                Assert.IsTrue(File.Exists(Config.SortedBlockTableFile("TestData\\TestManifestSnapshot", 1, 5)));
                Assert.IsTrue(File.Exists(Config.SortedBlockTableFile("TestData\\TestManifestSnapshot", 1, 6)));
            }

            // The files should be deleted now since we closed the snapshot
            Assert.IsFalse(File.Exists(Config.SortedBlockTableFile("TestData\\TestManifestSnapshot", 1, 4)));
            Assert.IsTrue(File.Exists(Config.SortedBlockTableFile("TestData\\TestManifestSnapshot", 1, 5)));
            Assert.IsFalse(File.Exists(Config.SortedBlockTableFile("TestData\\TestManifestSnapshot", 1, 6)));

        }

        [Test]
        public void TestManifestFileRollover() {

            var path = Path.GetFullPath("TestData\\TestManifestFileRollover");
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            // Remove the file if it exists
            var filename = Config.ManifestFile(path);
            if (File.Exists(filename))
                File.Delete(filename);

            var mf = new Manifest(path);
            for (int i = 0; i < Config.ManifestVersionCount - 10; i++) {
                var level = mf.NextVersion(1);
				level.GetType();
            }

            var manifestSize = new FileInfo(filename).Length;

            for (int i = 0; i < 30; i++) {
                var level = mf.NextVersion(1);
				level.GetType();
            }

            var newManifestSize = new FileInfo(filename).Length;

            // The new file should be smaller than the previous one, because it should have rolled over.
            Assert.Less(newManifestSize, manifestSize);
        }
    }
}
