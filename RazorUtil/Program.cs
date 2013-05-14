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
using System.IO;
using RazorDB;

namespace RazorUtil {

    public class Program {

        static void Main(string[] args) {
            Console.WriteLine("RazorDB Utility\n");

            if (args.Length == 0) {
                Console.WriteLine("Commands:");
                Console.WriteLine("\tdump-journal  <basedir> <version>");
                Console.WriteLine("\tdump-table <basedir> <level> <version>");
                Console.WriteLine("\tdump-manifest <manifest file> ");
                Console.WriteLine("\tdump-manifest-all <basedir>");
                Console.WriteLine("\tsplit-manifest <basedir>");
                Console.WriteLine("\tcheck-each-table <basedir>");
                Console.WriteLine("\tcheck-database <basedir>");
                Console.WriteLine("\tremove-orphans <basedir>");
                Console.WriteLine("\tremove-page <basedir> <level> <version>");
            } else {
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
                    case "dump-manifest-all":
                        if (args.Length < 2) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            var dummyMf = Manifest.NewDummyManifest();
                            dummyMf.Logger = msg => Console.WriteLine(msg);
                            foreach (var mf in Manifest.ReadAllManifests(args[1])) {
                                mf.LogContents(dummyMf);
                            }
                        }
                        break;
                    case "split-manifest":
                        if (args.Length < 2) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            var dummyMf = Manifest.NewDummyManifest();
                            dummyMf.Logger = msg => Console.WriteLine(msg);
                            int ct = 0;
                            foreach (var mf in Manifest.ReadAllManifests(args[1])) {
                                using (var bw = new BinaryWriter(new FileStream(Path.Combine(args[1], "S" + ct.ToString() + ".mf"), FileMode.CreateNew, FileAccess.Write, FileShare.None, 40096))) {
                                    mf.WriteManifestContents(bw);
                                }
                                ct++;
                            }
                        }
                        break;
                    case "check-each-table":
                        if (args.Length < 2) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            CheckBlockTableFiles(args[1]);
                        }
                        break;
                    case "check-database":
                        if (args.Length < 2) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            CheckDatabase(args[1]);
                        }
                        break;
                    case "remove-orphans":
                        if (args.Length < 2) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            RemoveOrphanedTables(args[1]);
                        }
                        break;
                    case "remove-page":
                        if (args.Length < 4) {
                            Console.WriteLine("Invalid parameters");
                        } else {
                            var pageRef = new PageRef { Level = int.Parse(args[2]), Version = int.Parse(args[3]) };
                            var mf = new Manifest(args[1]);
                            mf.ModifyPages(new List<PageRecord>(), new List<PageRef> { { pageRef } });
                        }
                        break;
                    default:
                        Console.WriteLine("Unknown command: {0}", args[0]);
                        break;
                }
            }

        }

        static void CheckBlockTableFiles(string baseDir) {
            Console.WriteLine("Checking Block Table Files '{0}'", baseDir);

            RazorCache cache = new RazorCache();
            foreach (string file in Directory.GetFiles(baseDir, "*.sbt", SearchOption.TopDirectoryOnly)) {
                var fileparts = Path.GetFileNameWithoutExtension(file).Split('-');
                int level = int.Parse(fileparts[0]);
                int version = int.Parse(fileparts[1]);

                Console.WriteLine("Level: {0} Version: {1}", level, version);

                var tablefile = new SortedBlockTable(cache, baseDir, level, version);
                try {
                    tablefile.ScanCheck();
                } finally {
                    tablefile.Close();
                }
            }
        }

        static void CheckDatabase(string baseDir) {
            Console.WriteLine("Checking Key Value Store '{0}'", baseDir);

            RazorCache cache = new RazorCache();
            var kv = new KeyValueStore(baseDir, cache);
            try {
                kv.ScanCheck();
            } finally {
                kv.Close();
            }
        }

        static void RemoveOrphanedTables(string baseDir) {
            Console.WriteLine("Removing Orphaned Tables '{0}'", baseDir);

            RazorCache cache = new RazorCache();
            var kv = new KeyValueStore(baseDir, cache);
            kv.Manifest.Logger = (msg) => Console.WriteLine(msg);

            try {
                kv.RemoveOrphanedPages();
            } finally {
                kv.Close();
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
