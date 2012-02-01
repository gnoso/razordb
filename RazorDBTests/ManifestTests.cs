using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;
using System.IO;
using System.Diagnostics;

namespace RazorDBTests {

    [TestFixture]
    public class ManifestTests {

        [Test]
        public void WriteAndReadManifest() {

            var path = Path.GetFullPath("WriteAndReadManifest");

            // Remove the file if it exists
            var filename = Path.ChangeExtension(path, "mf");
            if (File.Exists(filename)) 
                File.Delete(filename);

            var mf = new Manifest(path);
            Assert.AreEqual(path, mf.BaseFileName);
            Assert.AreEqual(0, mf.CurrentVersion(0));
            Assert.AreEqual(1, mf.NextVersion(0));
            Assert.AreEqual(1, mf.CurrentVersion(0));

            var mf2 = new Manifest(path);
            Assert.AreEqual(1, mf2.CurrentVersion(0));
            Assert.AreEqual(2, mf2.NextVersion(0));
            Assert.AreEqual(2, mf2.CurrentVersion(0));
        }

        [Test]
        public void WriteAndReadManifestMany() {

            var path = Path.GetFullPath("WriteAndReadManifestMany");

            // Remove the file if it exists
            var filename = Path.ChangeExtension(path, "mf");
            if (File.Exists(filename))
                File.Delete(filename);

            var mf = new Manifest(path);
            Assert.AreEqual(0, mf.CurrentVersion(0));

            var timer = new Stopwatch();
            timer.Start();
            for (int i = 0; i < 100; i++) {
                Assert.AreEqual(i+1, mf.NextVersion(0));
            }
            timer.Stop();

            Console.WriteLine("Committed manifest update in average of {0} ms", (double)timer.ElapsedMilliseconds / 100.0);

            var mf2 = new Manifest(path);
            Assert.AreEqual(100, mf2.CurrentVersion(0));
        }
    }
}
