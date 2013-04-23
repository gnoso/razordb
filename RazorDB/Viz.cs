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
