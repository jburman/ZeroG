#region License, Terms and Conditions
// Copyright (c) 2010 Jeremy Burman
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace ZeroG.Lang.JSON
{
    /// <summary>
    /// Represents a value that has been selected from the JSON data.
    /// </summary>
    public sealed class SelectedValue
    {
        /// <summary>
        /// The selector path that matched the value.
        /// </summary>
        public readonly string Path;
        /// <summary>
        /// The selected value itself.  Its type will be one of:
        /// Null, Boolean, String, Double
        /// </summary>
        public readonly object Value;
        /// <summary>
        /// The Element Index of the selected value within the inner-most Array that the 
        /// value is nested within.  Set to -1 if it is not nested within an Array.
        /// </summary>
        public readonly int Index;

        public SelectedValue(string path, object value, int index)
        {
            Path = path;
            Value = value;
            Index = index;
        }
    }

    /// <summary>
    /// Options to pass to the JSON selector.
    /// </summary>
    [Flags]
    public enum JSONSelectorOptions
    {
        None                = 0,
        /// <summary>
        /// Allow multiple values to be selected from within Arrays.  The default behavior is to ignore arrays.
        /// </summary>
        AllowMultiValue     = 2
    }

    /// <summary>
    /// Provides helper methods for JSONSelectorOptions.
    /// </summary>
    public static class JSONSelectorOptionsExtensions
    {
        public static bool AllowMultiValue(this JSONSelectorOptions options)
        {
            return options.IsOptionSet(JSONSelectorOptions.AllowMultiValue);
        }

        private static bool IsOptionSet(this JSONSelectorOptions options, JSONSelectorOptions flagToCheck)
        {
            return (flagToCheck == (flagToCheck & options));
        }
    }

    /// <summary>
    /// Provides a simple selector implementation for retrieving values from JSON data. 
    /// </summary>
    /// <example>
    /// TextReader jsonTextReader = new StringReader("{\"foo\":{\"bar\":\"baz\"}}");
    /// string[] selections = new string[] { "foo.bar" };
    /// SelectedValue[] values = JSONSelector.SelectValues(jsonTextReader, selections);
    /// </example>
    public sealed class JSONSelector
    {
        public const string ArraySpecifier = "[]";

        public static readonly JSONSelectorOptions DefaultOptions = JSONSelectorOptions.AllowMultiValue;

        private JSONWalkingEvents _events;
        private Dictionary<string, string> _paths;
        private List<SelectedValue> _selectedValues;
        private JSONSelectorOptions _options;

        private Stack<string> _objectPathStack;
        private string _objectPath;
        private string _memberPath;
        private int _arrayIndex;
        private Stack<int> _arrayIndexStack;

        public JSONSelector(JSONWalkingEvents events, string[] paths)
            : this(events, paths, DefaultOptions)
        {
        }

        public JSONSelector(JSONWalkingEvents events, string[] paths, JSONSelectorOptions options)
        {
            if (null == events)
            {
                throw new ArgumentNullException("events");
            }


            _events                 = events;
            paths                   = paths ?? new string[0];

            if (!options.AllowMultiValue())
            {
                // Filter out paths containing array specifiers
                _paths              = paths.Where(p => -1 == p.IndexOf(ArraySpecifier))
                                        .ToDictionary<string, string>(p => p);
            }
            else
            {
                _paths              = paths.ToDictionary<string, string>(p => p);
            }

            
            _selectedValues         = new List<SelectedValue>();
            _options                = options;

            _objectPathStack        = new Stack<string>();
            _objectPath             = string.Empty;
            _memberPath             = string.Empty;

            _arrayIndex             = -1;
            _arrayIndexStack        = new Stack<int>();

            _events.ObjectStart     += new JSONEventHandler(_events_ObjectStart);
            _events.ObjectEnd       += new JSONEventHandler(_events_ObjectEnd);
            _events.ObjectKey       += new JSONEventHandler<string>(_events_ObjectKey);

            _events.ArrayStart      += new JSONEventHandler(_events_ArrayStart);
            _events.ArrayEnd        += new JSONEventHandler(_events_ArrayEnd);
            _events.ArrayNext       += new JSONEventHandler(_events_ArrayNext);

            _events.String          += new JSONEventHandler<string>(_events_String);
            _events.Number          += new JSONEventHandler<double>(_events_Number);
            _events.Null            += new JSONEventHandler(_events_Null);
            _events.Boolean         += new JSONEventHandler<bool>(_events_Boolean);
        }

        #region static helpers
        public static SelectedValue[] SelectValues(string json, string[] selectors)
        {
            return SelectValues(json, selectors, DefaultOptions);
        }

        public static SelectedValue[] SelectValues(string json, string[] selectors, JSONSelectorOptions options)
        {
            if (string.IsNullOrEmpty(json))
            {
                return new SelectedValue[0];
            }

            return SelectValues(new StringReader(json), selectors, options);
        }

        public static SelectedValue[] SelectValues(TextReader json, string[] selectors)
        {
            return SelectValues(json, selectors, DefaultOptions);
        }
        
        public static SelectedValue[] SelectValues(TextReader json, string[] selectors, JSONSelectorOptions options)
        {
            if (null == json)
            {
                return new SelectedValue[0];
            }

            // Create tokenizer for the JSON text - this is fed to a Tree Walking Validator
            JSONTokenizer tokenizer = new JSONTokenizer(json);
            
            // Create an events object for the Tree Walker - events are fired as the walker 
            // traverses nodes in the tree.
            JSONWalkingEvents events = new JSONWalkingEvents();

            // The selector subscribes to the Walker Events and selects the appropriate values from the 
            // JSON tree.
            JSONSelector selector = new JSONSelector(events, selectors);

            // A JSON Tree Walker that validates the structure of the JSON and also fires events 
            // while traversing the tree.
            JSONWalkingValidator walker = new JSONWalkingValidator();
            walker.Walk(tokenizer.GetEnumerator(), events);

            // Retrieve the values selected from the JSON.
            return selector.SelectedValues;
        }
        #endregion

        #region Private implementation methods
        private void _consumeValue(object value)
        {
            if (_paths.ContainsKey(_memberPath))
            {
                _selectedValues.Add(
                    new SelectedValue(_memberPath, value, _arrayIndex));
            }
        }

        private void _events_Boolean(bool value)
        {
            _consumeValue(value);
        }

        private void _events_Null()
        {
            _consumeValue(null);
        }

        private void _events_String(string value)
        {
            _consumeValue(value);
        }

        private void _events_Number(double value)
        {
            _consumeValue(value);
        }

        private void _events_ArrayEnd()
        {
            _arrayIndex = _arrayIndexStack.Pop();

            _objectPath = _objectPathStack.Pop();
            _memberPath = string.Empty;
        }

        private void _events_ArrayStart()
        {
            _arrayIndexStack.Push(_arrayIndex);
            _arrayIndex = 0;

            _objectPathStack.Push(_objectPath);

            _objectPath = _memberPath + ArraySpecifier;
            _memberPath = _objectPath;
        }

        private void _events_ArrayNext()
        {
            ++_arrayIndex;
        }

        private void _events_ObjectKey(string value)
        {
            if (string.Empty != _objectPath)
            {
                _memberPath = _objectPath + "." + value;
            }
            else
            {
                _memberPath = value;
            }
        }

        private void _events_ObjectEnd()
        {
            _objectPath = _objectPathStack.Pop();
            _memberPath = _objectPath;
        }

        private void _events_ObjectStart()
        {
            _objectPathStack.Push(_objectPath);
            _objectPath = _memberPath;
        }
        #endregion

        public SelectedValue[] SelectedValues
        {
            get
            {
                return _selectedValues.ToArray();
            }
        }
    }
}
