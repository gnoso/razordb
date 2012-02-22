using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RazorDB;

namespace RazorUtil {

    public class Program {
        
        static void Main(string[] args) {
            Console.WriteLine("RazorDB Utility\n");

            if (args.Length > 0) {
                switch (args[0].ToLower()) {
                    case "dump-journal":
                        if (args.Length < 3) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            DumpJournal(args[1], int.Parse(args[2]));
                        }
                        break;
                    case "dump-table":
                        if (args.Length < 4) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            DumpFile(args[1], int.Parse(args[2]), int.Parse(args[3]));
                        }
                        break;
                    case "dump-manifest":
                        if (args.Length < 2) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            var mf = new Manifest(args[1]);
                            mf.Logger = msg => Console.WriteLine(msg);
                            mf.LogContents();
                        }
                        break;
                    default:
                        Console.WriteLine("Unknown command: {0}",args[0]);
                    break;
                }
            }

        }

        static void DumpFile(string baseDir, int level, int version) {
            RazorCache cache = new RazorCache();
            var tablefile = new SortedBlockTable(cache, baseDir, level, version);
            try {
                tablefile.DumpContents(msg => Console.WriteLine(msg));
            } finally {
                tablefile.Close();
            }
        }

        static void DumpJournal(string baseDir, int version) {
            var journal = new JournalReader(baseDir, version);
            Console.WriteLine("Journal\nBaseDir: {0} Version: {1}", baseDir, version);
            foreach (var pair in journal.Enumerate()) {
                Console.WriteLine("{0} => {1}", pair.Key.ToString(), pair.Value.ToString());
            }
        }
    }
}
