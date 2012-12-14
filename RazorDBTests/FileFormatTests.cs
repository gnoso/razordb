 ﻿using System;
 using System.Collections.Generic;
 using System.Linq;
 using System.Text;
 using NUnit.Framework;
 using RazorDB;
 using System.IO;
 using System.Diagnostics;
 
 namespace RazorDBTests {
 
     [TestFixture]
     public class OldFormatTests {
 
         [Test,Explicit("Used to generate a data for for backwards compatibility.")]
         public void CreateJournalFile() {
 
             string path = Path.GetFullPath("TestData\\JournalFileV1");
             if (!Directory.Exists(path))
                 Directory.CreateDirectory(path);
             JournalWriter jw = new JournalWriter(path, 324, false);
 
             List<KeyValuePair<Key, Value>> items = new List<KeyValuePair<Key, Value>>();
             for (int i = 0; i < 1000; i++) {
                 Key randKey = Key.Random(20);
                 Value randValue = Value.Random(100);
                 jw.Add(randKey, randValue);
                 items.Add(new KeyValuePair<Key, Value>(randKey, randValue));
             }
             jw.Close();
 
             JournalReader jr = new JournalReader(path, 324);
             int j = 0;
             foreach (var pair in jr.Enumerate()) {
                 Assert.AreEqual(items[j].Key, pair.Key);
                 Assert.AreEqual(items[j].Value, pair.Value);
                 j++;
             }
             jr.Close();
 
         }
 
         [Test, Explicit("Used to generate a data for for backwards compatibility.")]
         public void CreateManifestFile() {
 
             var path = Path.GetFullPath("TestData\\ManifestFileV1");
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
 
         [Test, Explicit("Used to generate a data for for backwards compatibility.")]
         public void CreateSBTFile() {
 
             string path = Path.GetFullPath("TestData\\SortedBlockTableV1");
             if (!Directory.Exists(path))
                 Directory.CreateDirectory(path);
 
             var mt = new MemTable();
             for (int i = 0; i < 1000; i++) {
                 var k0 = Key.Random(40);
                 var v0 = Value.Random(200);
                 mt.Add(k0, v0);
             }
 
             mt.WriteToSortedBlockTable("TestData\\SortedBlockTableV1", 0, 10);
 
             var cache = new RazorCache();
             var sbt = new SortedBlockTable(cache, "TestData\\SortedBlockTableV1", 0, 10);
 
             var timer = new Stopwatch();
             timer.Start();
             Assert.AreEqual(1000, sbt.Enumerate().Count());
             timer.Stop();
             Console.WriteLine("Counted sorted table at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
 
             // Confirm that the items are sorted.
             Key lastKey = Key.Empty;
             timer.Reset();
             timer.Start();
             foreach (var pair in sbt.Enumerate()) {
                 Assert.IsTrue(lastKey.CompareTo(pair.Key) < 0);
                 lastKey = pair.Key;
             }
             timer.Stop();
             Console.WriteLine("Read & verify sorted table at a throughput of {0} MB/s", (double)mt.Size / timer.Elapsed.TotalSeconds / (1024.0 * 1024.0));
 
             sbt.Close();
 
         }
 
         [Test]
         public void V1JournalFile() {
 
             string path = Path.GetFullPath(@"..\FormatTestData\V1");
 
             JournalReader jr = new JournalReader(path, 324);
             int j = 0;
             foreach (var pair in jr.Enumerate()) {
                 Assert.AreEqual( 20, pair.Key.Length);
                 Assert.AreEqual(100, pair.Value.Length);
                 j++;
             }
             jr.Close();
         }
 
         [Test]
         public void V1ManifestFile() {
 
             var path = Path.GetFullPath(@"..\FormatTestData\V1");
 
             var mf = new Manifest(path);
 
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
         public void V1SortedBlockTableFile() {
 
             var cache = new RazorCache();
             var sbt = new SortedBlockTable(cache, @"..\FormatTestData\V1", 0, 10);
 
             Assert.AreEqual(1000, sbt.Enumerate().Count());
 
             // Confirm that the items are sorted.
             Key lastKey = Key.Empty;
 
             foreach (var pair in sbt.Enumerate()) {
                 Assert.IsTrue(lastKey.CompareTo(pair.Key) < 0);
                 lastKey = pair.Key;
             }
 
             sbt.Close();
 
         }
 
     }
 }
