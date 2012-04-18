using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace RazorView {
    /// <summary>
    /// Interaction logic for FilterPanelControl.xaml
    /// </summary>
    public partial class FilterPanelControl : UserControl {
        public FilterPanelControl() {
            InitializeComponent();
        }

        public event EventHandler RefreshEventHandler;

        public string KeyFilter {
            get { return KeyFilterTextBox.Text; }
            set { KeyFilterTextBox.Text = value; }
        }

        public string ValueFilter {
            get { return ValueFilterTextBox.Text; }
            set { ValueFilterTextBox.Text = value; }
        }

        private void Button_Click(object sender, RoutedEventArgs e) {

            if (RefreshEventHandler != null)
                RefreshEventHandler(sender, e);
        }
    }
}
