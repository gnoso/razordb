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
                case "CrashTestBeforeMerge":
                    CrashTestBeforeMerge();
                    break;
            }

        }

        public static void CrashTestOnMerge() {

            string path = Path.GetFullPath("TestData\\CrashTestOnMerge");
            int num_items = 50000;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                for (int i = 0; i < num_items; i++) {
                    byte[] keyBytes = new byte[40];
                    Array.Copy(BitConverter.GetBytes(i).Reverse().ToArray(), keyBytes, 4);
                    Array.Copy(ByteArray.Random(36).InternalBytes, 0, keyBytes, 4, 36); 
                    var randomKey = new ByteArray(keyBytes);
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

        public static void CrashTestBeforeMerge() {

            string path = Path.GetFullPath("TestData\\CrashTestBeforeMerge");
            int num_items = 10000;

            using (var db = new KeyValueStore(path)) {
                db.Truncate();

                db.Manifest.Logger = (msg) => Console.WriteLine(msg);

                for (int i = 0; i < num_items; i++) {
                    var randomKey = ByteArray.Random(4);
                    var randomValue = ByteArray.Random(5);
                    db.Set(randomKey.InternalBytes, randomValue.InternalBytes);
                }

                // Signal our test to fall through
                try {
                    ManualResetEvent.OpenExisting("CrashTestBeforeMerge").Set();
                } catch (WaitHandleCannotBeOpenedException e) {
                    Console.WriteLine("{0}", e);
                }
            }

        }
    }
}
