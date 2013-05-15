/*
Copyright 2012, 2013 Taylor Shuler.

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
using System.IO;

namespace RazorDB
{
	public class ProjectDetector
	{
		public ProjectDetector ()
		{
		}

		public static void Main(string[] args)
		{
			using (var writer = new StreamWriter("RazorDB.sln"))
			{
				writer.WriteLine("Microsoft Visual Studio Solution File, Format Version 11.00");
				writer.WriteLine("# Visual Studio 2010");

				var seenElements = new HashSet<string>();

				foreach (var file in (new DirectoryInfo(System.IO.Directory.GetCurrentDirectory())).GetFiles("*.csproj", SearchOption.AllDirectories))
				{
					string fileName = Path.GetFileNameWithoutExtension(file.Name);

					if (seenElements.Add(fileName))
					{
						writer.WriteLine(string.Format(@"Project(""0"") = ""{0}"", ""{1}"",""{2}""", fileName, file.FullName, Guid.NewGuid()));
						writer.WriteLine("EndProject");
					}
				}
			}
		}
	}
}