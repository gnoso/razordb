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
                    case "dump":
                        if (args.Length < 4) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            DumpFile(args[1], int.Parse(args[2]), int.Parse(args[3]));
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
    }
}
