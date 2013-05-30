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
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace RazorDB {
    public class TableManager {

        private static TableManager _tableManagerInstance;
        private static object startupLock = new object();
        static TableManager() {
            lock (startupLock) {
                if (_tableManagerInstance == null)
                    _tableManagerInstance = new TableManager();
            }
        }

        public static TableManager Default {
            get { return _tableManagerInstance; }
        }

        private TableManager() {}

        private long pauseTime = Stopwatch.Frequency / 4;

        public void MarkKeyValueStoreAsModified(KeyValueStore kvStore) {
            
            // Only schedule a merge run if no merging is happening
            if (kvStore.mergeCount == 0) {

                // determine if we've reached the next time threshold for another update
                long ticks = Stopwatch.GetTimestamp();
                long ticksTillNext = kvStore.ticksTillNextMerge;
                if (ticks > ticksTillNext) {
                    // Schedule a tablemerge run on the threadpool
                    ThreadPool.QueueUserWorkItem((o) => {
                        RunTableMergePass(kvStore);
                    });
                }
                kvStore.ticksTillNextMerge = ticks + pauseTime;
            }
        }

        public void Close(KeyValueStore kvStore) {
            RunTableMergePass(kvStore);
        }

        public static void RunTableMergePass(KeyValueStore kvStore) {

            try {
                Interlocked.Increment(ref kvStore.mergeCount);

                lock (kvStore.mergeLock) {
                    RazorCache cache = kvStore.Cache;
                    Manifest manifest = kvStore.Manifest;

                    while (true) {
                        bool mergedDuringLastPass = false;
                        using (var manifestInst = kvStore.Manifest.GetLatestManifest()) {
                            // Handle level 0 (merge all pages)
                            if (manifestInst.GetNumPagesAtLevel(0) >= Config.MaxPagesOnLevel(0)) {
                                mergedDuringLastPass = true;
                                int Level0PagesToTake = Config.MaxPagesOnLevel(0) * 2; // Grab more pages if they are available (this happens during heavy write pressure)
                                var inputPageRecords = manifestInst.GetPagesAtLevel(0).OrderBy(p => p.Version).Take(Level0PagesToTake).ToList();
                                var startKey = inputPageRecords.Min(p => p.FirstKey);
                                var endKey = inputPageRecords.Max(p => p.LastKey);
                                var mergePages = manifestInst.FindPagesForKeyRange(1, startKey, endKey).AsPageRefs().ToList();
                                var allInputPages = inputPageRecords.AsPageRefs().Concat(mergePages).ToList();

                                var outputPages = SortedBlockTable.MergeTables(cache, manifest, 1, allInputPages, ExceptionHandling.ThrowAll, null).ToList();
                                manifest.ModifyPages(outputPages, allInputPages);

                                manifest.LogMessage("Merge Level 0 => InputPages: {0} OutputPages:{1}",
                                    string.Join(",", allInputPages.Select(p => string.Format("{0}-{1}", p.Level, p.Version)).ToArray()),
                                    string.Join(",", outputPages.Select(p => string.Format("{0}-{1}", p.Level, p.Version)).ToArray())
                                );
                            }
                            // handle the rest of the levels (merge only one page upwards)
                            for (int level = 1; level < manifestInst.NumLevels - 1; level++) {
                                if (manifestInst.GetNumPagesAtLevel(level) >= Config.MaxPagesOnLevel(level)) {
                                    mergedDuringLastPass = true;
                                    var inputPage = manifest.NextMergePage(level);
                                    var mergePages = manifestInst.FindPagesForKeyRange(level + 1, inputPage.FirstKey, inputPage.LastKey).ToList();
                                    var inputPageRecords = mergePages.Concat(new PageRecord[] { inputPage });
                                    var allInputPages = inputPageRecords.AsPageRefs().ToList();
                                    var outputPages = SortedBlockTable.MergeTables(cache, manifest, level + 1, allInputPages, ExceptionHandling.ThrowAll, null);

                                    // Notify if a merge happened, implemented for testing primarily
                                    if (kvStore.MergeCallback != null) kvStore.MergeCallback(level, inputPageRecords, outputPages);
                                    manifest.ModifyPages(outputPages, allInputPages);

                                    manifest.LogMessage("Merge Level >0 => InputPages: {0} OutputPages:{1}",
                                        string.Join(",", allInputPages.Select(p => string.Format("{0}-{1}", p.Level, p.Version)).ToArray()),
                                        string.Join(",", outputPages.Select(p => string.Format("{0}-{1}", p.Level, p.Version)).ToArray())
                                    );
                                }
                            }
                        }

                        // No more merging is needed, we are finished with this pass
                        if (!mergedDuringLastPass)
                            return;
                    }
                }
            } finally {
                Interlocked.Decrement(ref kvStore.mergeCount);
            }
        }

    }
}
