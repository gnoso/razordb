using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Diagnostics;
using System.Threading;
using System.IO;
using RazorDB;
using System.Reflection;

namespace RazorDBTests {

    [TestFixture]
    public class CrashTests {

        [Test]
        public void CrashTestOnMerge() {

            string path = Path.GetFullPath("TestData\\CrashTestOnMerge");
            using (var db = new KeyValueStore(path)) {
                db.Truncate();
            }

            var doneSetting = new EventWaitHandle(false, EventResetMode.ManualReset, "CrashTestOnMerge");
            doneSetting.Reset();

            string testPath = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().GetName().CodeBase), "RazorTest.exe");
            var process = Process.Start(testPath, "CrashTestOnMerge");

            doneSetting.WaitOne(30000);
            process.Kill();
            process.WaitForExit();
            
            // Open the database created by the other program
            using (var db = new KeyValueStore(path)) {

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                Console.WriteLine("Begin enumeration.");
                ByteArray lastKey = new ByteArray();
                int ct = 0;
                foreach (var pair in db.Enumerate()) {
                    ByteArray k = new ByteArray(pair.Key);
                    ByteArray v = new ByteArray(pair.Value);
                    Assert.True(lastKey.CompareTo(k) < 0);
                    lastKey = k;
                    ct++;
                }
                Assert.AreEqual(100000, ct);
                Console.WriteLine("Found {0} items in the crashed database.", ct);
            }

        }

        [Test]
        public void CrashTestBeforeMerge() {

            string path = Path.GetFullPath("TestData\\CrashTestBeforeMerge");
            using (var db = new KeyValueStore(path)) {
                db.Truncate();
            }

            var doneSetting = new EventWaitHandle(false, EventResetMode.ManualReset, "CrashTestBeforeMerge");
            doneSetting.Reset();

            string testPath = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().GetName().CodeBase), "RazorTest.exe");
            var process = Process.Start(testPath, "CrashTestBeforeMerge");

            doneSetting.WaitOne(30000);
            process.Kill();
            process.WaitForExit();

            // Open the database created by the other program
            using (var db = new KeyValueStore(path)) {

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                Console.WriteLine("Begin enumeration.");
                ByteArray lastKey = new ByteArray();
                int ct = 0;
                foreach (var pair in db.Enumerate()) {
                    ByteArray k = new ByteArray(pair.Key);
                    ByteArray v = new ByteArray(pair.Value);
                    Assert.True(lastKey.CompareTo(k) < 0);
                    lastKey = k;
                    ct++;
                }
                Assert.AreEqual(10000, ct);
                Console.WriteLine("Found {0} items in the crashed database.", ct);
            }

        }

    }
}
