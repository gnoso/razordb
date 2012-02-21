using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using RazorDB;
using System.Threading;

namespace RazorTest {
    public class Program {

        public static void Main(string[] args) {

            if (args.Length != 1)
                return;

            switch (args[0]) {
                case "CrashTestOnMerge":
                    CrashTestOnMerge();
                    break;
            }

        }

        public static void CrashTestOnMerge() {

            string path = Path.GetFullPath("TestData\\CrashTestOnMerge");
            int num_items = 100000;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                for (int i = 0; i < num_items; i++) {
                    var randomKey = ByteArray.Random(40);
                    var randomValue = ByteArray.Random(256);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);
                }

                // Signal our test to fall through
                try {
                    ManualResetEvent.OpenExisting("CrashTestOnMerge").Set();
                } catch (WaitHandleCannotBeOpenedException e) {
                    Console.WriteLine("{0}", e);
                }
            }

        }
    }
}
