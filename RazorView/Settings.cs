using System.Collections.Generic;
namespace RazorView.Properties {
    
    
    // This class allows you to handle specific events on the settings class:
    //  The SettingChanging event is raised before a setting's value is changed.
    //  The PropertyChanged event is raised after a setting's value is changed.
    //  The SettingsLoaded event is raised after the setting values are loaded.
    //  The SettingsSaving event is raised before the setting values are saved.
    public sealed partial class Settings {
        
        public Settings() {
            // // To add event handlers for saving and changing settings, uncomment the lines below:
            //
            // this.SettingChanging += this.SettingChangingEventHandler;
            //
            // this.SettingsSaving += this.SettingsSavingEventHandler;
            //
        }
        
        void SettingChangingEventHandler(object sender, System.Configuration.SettingChangingEventArgs e) {
            // Add code to handle the SettingChangingEvent event here.
        }
        
        void SettingsSavingEventHandler(object sender, System.ComponentModel.CancelEventArgs e) {
            // Add code to handle the SettingsSaving event here.
        }

        public void AddVisualizerAssembly(string assemblyName) {
            List<string> list = VisualizerAssembliesList;
            list.Add(assemblyName);
            VisualizerAssembliesList = list;
        }

        public void RemoveVisualizerAssembly(string assemblyName) {
            List<string> list = VisualizerAssembliesList;
            list.Remove(assemblyName);
            VisualizerAssembliesList = list;
        }

        public List<string> VisualizerAssembliesList {
            get { return string.IsNullOrWhiteSpace(VisualizerAssemblies) ? new List<string>() : new List<string>(VisualizerAssemblies.Split(';')); }
            set { VisualizerAssemblies = string.Join(";", value); }
        }

    }
}
