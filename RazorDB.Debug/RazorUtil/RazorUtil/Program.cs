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
using RazorDB;
using System;
using System.Collections.Generic;
using System.IO;

namespace RazorUtil
{
	public class Program
	{
		Console c;

		static void Main (string[] args)
		{
			c.WriteLine ("RazorDB Utility\n");

			if (args.Length == 0) {
				c.WriteLine ("Commands:");
				c.WriteLine ("\tdump-journal  <basedir> <version>");
				c.WriteLine ("\tdump-table <basedir> <level> <version>");
				c.WriteLine ("\tdump-manifest <manifest file> ");
				c.WriteLine ("\tdump-manifest-all <basedir>");
				c.WriteLine ("\tsplit-manifest <basedir>");
				c.WriteLine ("\tcheck-each-table <basedir>");
				c.WriteLine ("\tcheck-database <basedir>");
				c.WriteLine ("\tremove-orphans <basedir>");
				c.WriteLine ("\tremove-page <basedir> <level> <version>");
			} else {
				switch (args [0].ToLower ()) {
				case "dump-journal":
					if (args.Length < 3) {
						c.WriteLine ("Invalid parameters");
					} else {
						DumpJournal (args[1], int.Parse (args[2]));
					}
					break;
				case "dump-table":
					if (args.Length < 4) {
						c.WriteLine ("Invalid parameters");
					} else {
						DumpFile (args[1], int.Parse (args[2]), int.Parse (args[3]));
					}
					break;
				case "dump-manifest":
					if (args.Length < 2) {
						c.WriteLine ("Invalid parameters");
					} else {
						var mf = new Manifest (args[1]);
						mf.Logger = msg => c.WriteLine (msg);
						mf.LogContents ();
					}
					break;
				case "dump-manifest-all":
					if (args.Length < 2) {
						c.WriteLine ("Invalid parameters");
					} else {
						var dummyMf = Manifest.NewDummyManifest ();
						dummyMf.Logger = msg => c.WriteLine (msg);
						foreach (var mf in Manifest.ReadAllManifests(args[1])) {
							mf.LogContents (dummyMf);
						}
					}
					break;
				case "split-manifest":
					if (args.Length < 2) {
						c.WriteLine ("Invalid parameters");
					} else {
						var dummyMf = Manifest.NewDummyManifest ();
						dummyMf.Logger = msg => c.WriteLine (msg);
						int ct = 0;
						foreach (var mf in Manifest.ReadAllManifests(args[1])) {
							using (var bw = new BinaryWriter(new FileStream(Path.Combine(args[1], "S" + ct.ToString() + ".mf"), FileMode.CreateNew, FileAccess.Write, FileShare.None, 40096))) {
								mf.WriteManifestContents (bw);
							}
							ct++;
						}
					}
					break;
				case "check-each-table":
					if (args.Length < 2) {
						c.WriteLine ("Invalid parameters");
					} else {
						CheckBlockTableFiles (args[1]);
					}
					break;
				case "check-database":
					if (args.Length < 2) {
						c.WriteLine ("Invalid parameters");
					} else {
						CheckDatabase (args[1]);
					}
					break;
				case "remove-orphans":
					if (args.Length < 2) {
						c.WriteLine ("Invalid parameters");
					} else {
						RemoveOrphanedTables (args[1]);
					}
					break;
				case "remove-page":
					if (args.Length < 4) {
						c.WriteLine ("Invalid parameters");
					} else {
						var pageRef = new PageRef { Level = int.Parse(args[2]), Version = int.Parse(args[3]) };
						var mf = new Manifest (args[1]);
						mf.ModifyPages (new List<PageRecord>(), new List<PageRef> { { pageRef } });
					}
					break;
				default:
					c.WriteLine ("Unknown command: {0}", args [0]);
					break;
				}
			}

		}

		static void CheckBlockTableFiles (string baseDir)
		{
			c.WriteLine ("Checking Block Table Files '{0}'", baseDir);

			RazorCache cache = new RazorCache ();
			foreach (string file in Directory.GetFiles(baseDir, "*.sbt", SearchOption.TopDirectoryOnly)) {
				var fileparts = Path.GetFileNameWithoutExtension (file).Split ('-');
				int level = int.Parse (fileparts[0]);
				int version = int.Parse (fileparts[1]);

				c.WriteLine ("Level: {0} Version: {1}", level, version);

				var tablefile = new SortedBlockTable (cache, baseDir, level, version);
				try {
					tablefile.ScanCheck ();
				} finally {
					tablefile.Close ();
				}
			}
		}

		static void CheckDatabase (string baseDir)
		{
			c.WriteLine ("Checking Key Value Store '{0}'", baseDir);

			RazorCache cache = new RazorCache ();
			var kv = new KeyValueStore (baseDir, cache);
			try {
				kv.ScanCheck ();
			} finally {
				kv.Close ();
			}
		}

		static void RemoveOrphanedTables (string baseDir)
		{
			c.WriteLine ("Removing Orphaned Tables '{0}'", baseDir);

			RazorCache cache = new RazorCache ();
			var kv = new KeyValueStore (baseDir, cache);
			kv.Manifest.Logger = (msg) => c.WriteLine (msg);

			try {
				kv.RemoveOrphanedPages ();
			} finally {
				kv.Close ();
			}
		}

		static void DumpFile (string baseDir, int level, int version)
		{
			RazorCache cache = new RazorCache ();
			var tablefile = new SortedBlockTable (cache, baseDir, level, version);
			try {
				tablefile.DumpContents (msg => c.WriteLine(msg));
			} finally {
				tablefile.Close ();
			}
		}

		static void DumpJournal (string baseDir, int version)
		{
			var journal = new JournalReader (baseDir, version);
			c.WriteLine ("Journal\nBaseDir: {0} Version: {1}", baseDir, version);
			foreach (var pair in journal.Enumerate()) {
				c.WriteLine ("{0} => {1}", pair.Key.ToString (), pair.Value.ToString ());
			}
		}
	}
}