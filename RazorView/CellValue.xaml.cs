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
    /// Interaction logic for CellValue.xaml
    /// </summary>
    public partial class CellValue : UserControl {
        public CellValue() {
            InitializeComponent();

            Expanded = false;
        }

        bool expanded = false;
        public bool Expanded {
            get { return expanded; }
            set { 
                expanded = value;
                if (expanded) {
                    TextBox.MaxHeight = Double.MaxValue;
                    ExpandButton.Content = "Collapse";
                } else {
                    TextBox.MaxHeight = 50;
                    ExpandButton.Content = "Expand";
                }
            }
        }

        void Button_Click(object sender, RoutedEventArgs e) {
            Expanded = !Expanded;
        }
    }
}
