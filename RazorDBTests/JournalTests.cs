using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RazorDB;
using System.IO;

namespace RazorDBTests {

    [TestFixture]
    public class JournalTests {

        [Test]
        public void ReadAndWriteJournalFile() {

            string path = Path.GetFullPath("RWJournal");
            JournalWriter jw = new JournalWriter(path, 324);

            List<KeyValuePair<ByteArray, ByteArray>> items = new List<KeyValuePair<ByteArray, ByteArray>>();
            for (int i = 0; i < 10000; i++) {
                ByteArray randKey = ByteArray.Random(20);
                ByteArray randValue = ByteArray.Random(100);
                jw.Add(randKey, randValue);
                items.Add( new KeyValuePair<ByteArray,ByteArray>(randKey, randValue) );
            }
            jw.Close();

            JournalReader jr = new JournalReader(path, 324);
            int j=0;
            foreach (var pair in jr.Enumerate()) {
                Assert.AreEqual( items[j].Key, pair.Key );
                Assert.AreEqual( items[j].Value, pair.Value );
                j++;
            }
            jr.Close();
        }

    }
}
