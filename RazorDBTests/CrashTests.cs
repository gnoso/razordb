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
                    Assert.True(lastKey.CompareTo(k) < 0);
                    lastKey = k;
                    ct++;
                }
                Assert.AreEqual(50000, ct);
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
            if (!process.HasExited) {
                process.Kill();
                process.WaitForExit();
            }

            // Open the database created by the other program
            using (var db = new KeyValueStore(path)) {

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                Console.WriteLine("Begin enumeration.");
                ByteArray lastKey = new ByteArray();
                int ct = 0;
                foreach (var pair in db.Enumerate()) {
                    ByteArray k = new ByteArray(pair.Key);
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
