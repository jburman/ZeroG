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

namespace ZeroG.Lang.BSON
{
    /// <summary>
    /// Represents a value that has been selected from the BSON data.
    /// </summary>
    public sealed class SelectedValue
    {
        /// <summary>
        /// The selector path that matched the value.
        /// </summary>
        public readonly string Path;
        /// <summary>
        /// The selected value itself.  Its type will be one of:
        /// Null, Boolean, String, Double, Int32, Int64, DateTime, Binary(bin[])
        /// </summary>
        public readonly object Value;
        /// <summary>
        /// The Element Index of the selected value within the inner-most Array that the 
        /// value is nested within.  Set to -1 if it is not nested within an Array.
        /// </summary>
        public readonly int Index;

        public SelectedValue(string path, object value, int index)
        {
            Path    = path;
            Value   = value;
            Index   = index;
        }
    }

    /// <summary>
    /// Options to pass to the BSON selector.
    /// </summary>
    [Flags]
    public enum BSONSelectorOptions
    {
        None                = 0,
        /// <summary>
        /// Force arrays to be ignored in the selectors.
        /// </summary>
        IgnoreArrays        = 2
    }

    /// <summary>
    /// Provides helper methods for BSONSelectorOptions.
    /// </summary>
    public static class BSONSelectorOptionsExtensions
    {
        public static bool IgnoreArrays(this BSONSelectorOptions options)
        {
            return options.IsOptionSet(BSONSelectorOptions.IgnoreArrays);
        }

        private static bool IsOptionSet(this BSONSelectorOptions options, BSONSelectorOptions flagToCheck)
        {
            return (flagToCheck == (flagToCheck & options));
        }
    }

    /// <summary>
    /// Provides a simple selector implementation for retrieving values from BSON data. For more advanced 
    /// selector needs, please refer to the BSONPathSelector instead.
    /// </summary>
    /// <example>
    /// string[] selections = new string[] { "foo.bar" };
    /// SelectedValue[] values = BSONSelector.SelectValues(binaryReader, selections);
    /// </example>
    /// <seealso cref="ZeroG.Lang.BSON.BSONPathSelector" />
    public sealed class BSONSelector
    {
        public const string ArraySpecifier = "[]";

        public static readonly BSONSelectorOptions DefaultOptions = BSONSelectorOptions.None;

        private BSONWalkingEvents _events;
        private Dictionary<string, string> _paths;
        private List<SelectedValue> _selectedValues;
        private BSONSelectorOptions _options;

        private Stack<string> _documentPathStack;
        private Stack<BSONTypes> _docTypeStack;
        private string _docPath;
        private string _memberPath;
        private int _arrayIndex;
        private Stack<int> _arrayIndexStack;

        public BSONSelector(BSONWalkingEvents events, string[] paths)
            : this(events, paths, DefaultOptions)
        {
        }

        public BSONSelector(BSONWalkingEvents events, string[] paths, BSONSelectorOptions options)
        {
            if (null == events)
            {
                throw new ArgumentNullException("events");
            }

            _events                 = events;
            paths                   = paths ?? new string[0];

            if (options.IgnoreArrays())
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

            _documentPathStack      = new Stack<string>();
            _docTypeStack           = new Stack<BSONTypes>();
            _docPath                = string.Empty;
            _memberPath             = string.Empty;

            _arrayIndex             = -1;
            _arrayIndexStack        = new Stack<int>();

            _events.DocumentStart   += new BSONEventHandler(_events_DocumentStart);
            _events.DocumentEnd     += new BSONEventHandler(_events_DocumentEnd);
            _events.Element         += new BSONElementEventHandler(_events_Element);

            _events.ArrayStart      += new BSONEventHandler(_events_ArrayStart);
            _events.ArrayEnd        += new BSONEventHandler(_events_ArrayEnd);

            _events.String          += new BSONValueEventHandler<string>(_events_String);
            _events.Int32           += new BSONValueEventHandler<Int32>(_events_Int32);
            _events.Int64           += new BSONValueEventHandler<Int64>(_events_Int64);
            _events.Double          += new BSONValueEventHandler<double>(_events_Double);
            _events.DateTime        += new BSONValueEventHandler<DateTime>(_events_DateTime);
            _events.Value           += new BSONValueEventHandler(_events_Value);
            _events.Null            += new BSONEventHandler(_events_Null);
            _events.Boolean         += new BSONValueEventHandler<bool>(_events_Boolean);
        }

        #region static helpers
        public static SelectedValue[] SelectValues(byte[] bson, string[] selectors)
        {
            return SelectValues(bson, selectors, DefaultOptions);
        }

        public static SelectedValue[] SelectValues(byte[] bson, string[] selectors, BSONSelectorOptions options)
        {
            if (null == bson)
            {
                return new SelectedValue[0];
            }

            return SelectValues(new BinaryReader(new MemoryStream(bson)), selectors, options);
        }

        public static SelectedValue[] SelectValues(BinaryReader bson, string[] selectors)
        {
            return SelectValues(bson, selectors, DefaultOptions);
        }
        
        public static SelectedValue[] SelectValues(BinaryReader bson, string[] selectors, BSONSelectorOptions options)
        {
            if (null == bson)
            {
                return new SelectedValue[0];
            }

            // Create tokenizer for the BSON document - this is fed to a Tree Walking Validator
            BSONTokenizer tokenizer = new BSONTokenizer(bson);
            
            // Create an events object for the Tree Walker - events are fired as the walker 
            // traverses nodes in the tree.
            BSONWalkingEvents events = new BSONWalkingEvents();

            // The selector subscribes to the Walker Events and selects the appropriate values from the 
            // BSON tree.
            BSONSelector selector = new BSONSelector(events, selectors);

            // A BSON Tree Walker that validates the structure of the BSON and also fires events 
            // while traversing the document.
            BSONWalkingValidator walker = new BSONWalkingValidator();
            walker.Walk(tokenizer.GetEnumerator(), events);

            // Retrieve the values selected from the BSON.
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

        private void _events_Double(double value)
        {
            _consumeValue(value);
        }

        private void _events_Int32(Int32 value)
        {
            _consumeValue(value);
        }

        private void _events_Int64(Int64 value)
        {
            _consumeValue(value);
        }

        private void _events_DateTime(DateTime value)
        {
            _consumeValue(value);
        }

        private void _events_Value(BSONToken token)
        {
            if (BSONTokenType.BINARY == token.Type) // only supports the Binary type
            {
                _consumeValue(((BSONBinaryToken)token).Value);
            }
        }

        private void _events_ArrayEnd()
        {
            _arrayIndex = _arrayIndexStack.Pop();

            _docPath = _documentPathStack.Pop();
            _docTypeStack.Pop();
            _memberPath = string.Empty;
        }

        private void _events_ArrayStart()
        {
            _arrayIndexStack.Push(_arrayIndex);
            _arrayIndex = 0;

            _documentPathStack.Push(_docPath);
            _docTypeStack.Push(BSONTypes.ARRAY);

            _docPath = _memberPath + ArraySpecifier;
            _memberPath = _docPath;
        }

        private void _events_Element(string name, BSONTokenType type)
        {
            if (BSONTypes.ARRAY == _docTypeStack.Peek())
            {
                _arrayIndex = int.Parse(name);
            }
            else
            {
                if (string.Empty != _docPath)
                {
                    _memberPath = _docPath + "." + name;
                }
                else
                {
                    _memberPath = name;
                }
            }
        }

        private void _events_DocumentEnd()
        {
            _docPath = _documentPathStack.Pop();
            _docTypeStack.Pop();
            _memberPath = _docPath;
        }

        private void _events_DocumentStart()
        {
            _documentPathStack.Push(_docPath);
            _docTypeStack.Push(BSONTypes.DOCUMENT);
            _docPath = _memberPath;
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
