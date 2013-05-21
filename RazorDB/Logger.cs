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

namespace RazorDB
{
	public static class Logger
	{
		static Action<string> _log;

		/* Here is an example of/for assigning an observeratory Logger:
		 * 	Logger.log = x => Console.WriteLine(x);
		 * 
		 * If the log action isn't set, a dummy action will be returned, eliminating the need for null reference checks:
		 * 	(x => {return;});
		 */

		// Gets or sets the log
		public static Action<string> log
		{
			get {
				return _log ?? (x => {
					return;
				});
			}

			set {
				_log = value;
			}
		}
	}
}