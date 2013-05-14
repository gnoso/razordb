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

namespace RazorDB {

    // This interface is used by the RazorView program to allow an assembly to define possibly better ways of visualizing the data in the key value store
    public interface IDataVizFactory {
        IDataViz GetVisualizer(KeyValueStore kvStore);
    }

    public interface IDataViz {
        string TransformKey(byte[] key);
        string TransformValue(byte[] value);
    }
}
