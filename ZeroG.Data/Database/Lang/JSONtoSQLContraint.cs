#region License, Terms and Conditions
// Copyright (c) 2012 Jeremy Burman
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
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using ZeroG.Lang.JSON;

namespace ZeroG.Data.Database.Lang
{
    public enum ConstraintKeywords
    {
        Unknown,
        Op,
        And,
        Or
    }

    public enum ConstraintOperator
    {
        NotSet,
        Equals,
        NotEquals,
        Like,
        NotLike,
        In,
        NotIn,
        LessThan,
        LessThanOrEquals,
        GreaterThan,
        GreaterThanOrEquals
    }

    public enum ConstraintLogic
    {
        NotSet,
        And,
        Or
    }

    public class Constraint
    {
        public string Name;
        public object Value;
        public string LastKey;
        public ConstraintOperator Operator;
        public ConstraintLogic Logic;
        public ConstraintKeywords LastKeyword;
        public bool InKeyword;
        public bool InArray;
        public List<object> ArrayValues;
        public List<Constraint> Constraints;
    }

    public class SQLConstraint
    {
        public SQLConstraint(string sql, IEnumerable<IDataParameter> parameters)
        {
            SQL = sql;
            Parameters = parameters;
        }

        public string SQL { get; private set; }
        public IEnumerable<IDataParameter> Parameters { get; private set; }
    }

    public class JSONToSQLConstraint
    {
        private const int _MaxConstraint = 100;
        private static Type _ConstraintKeywordsType = typeof(ConstraintKeywords);
        private static Dictionary<string, ConstraintOperator> _operators;
        private static Dictionary<ConstraintOperator, string> _operatorsReverse;

        private IDatabaseService _db;
        private JSONWalkingEvents _events;
        private Dictionary<string, Type> _typeMappings;
        private Stack<Constraint> _constraints;
        private Constraint _constraint;
        private HashSet<string> _constraintLogic;

        static JSONToSQLConstraint()
        {
            _operators = new Dictionary<string, ConstraintOperator>(StringComparer.InvariantCultureIgnoreCase);
            _operators["="] = ConstraintOperator.Equals;
            _operators["<>"] = ConstraintOperator.NotEquals;
            _operators["LIKE"] = ConstraintOperator.Like;
            _operators["NOT LIKE"] = ConstraintOperator.NotLike;
            _operators["IN"] = ConstraintOperator.In;
            _operators["NOT IN"] = ConstraintOperator.NotIn;
            _operators["<"] = ConstraintOperator.LessThan;
            _operators["<="] = ConstraintOperator.LessThanOrEquals;
            _operators[">"] = ConstraintOperator.GreaterThan;
            _operators[">="] = ConstraintOperator.GreaterThanOrEquals;

            _operatorsReverse = new Dictionary<ConstraintOperator, string>();

            foreach (var op in _operators)
            {
                _operatorsReverse[op.Value] = op.Key;
            }
        }

        public JSONToSQLConstraint(IDatabaseService db, JSONWalkingEvents events, Dictionary<string, Type> typeMappings)
        {
            if (null == db)
            {
                throw new ArgumentNullException("db");
            }

            if (null == events)
            {
                throw new ArgumentNullException("events");
            }

            if (null == typeMappings)
            {
                throw new ArgumentNullException("typeMappings");
            }

            _db = db;
            _events = events;
            _typeMappings = typeMappings;

            _events.ObjectStart += new JSONEventHandler(_events_ObjectStart);
            _events.ObjectEnd += new JSONEventHandler(_events_ObjectEnd);
            _events.ObjectKey += new JSONEventHandler<string>(_events_ObjectKey);

            _events.ArrayStart += new JSONEventHandler(_events_ArrayStart);
            _events.ArrayEnd += new JSONEventHandler(_events_ArrayEnd);
            _events.ArrayNext += new JSONEventHandler(_events_ArrayNext);

            _events.String += new JSONEventHandler<string>(_events_String);
            _events.Number += new JSONEventHandler<double>(_events_Number);
            _events.Null += new JSONEventHandler(_events_Null);
            _events.Boolean += new JSONEventHandler<bool>(_events_Boolean);

            _constraints = new Stack<Constraint>();
            _constraintLogic = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);

            var names = Enum.GetNames(typeof(ConstraintLogic));
            foreach (var name in names)
            {
                _constraintLogic.Add(name);
            }
        }

        #region Static helpers
        public static SQLConstraint GenerateSQLConstraint(IDatabaseService db, Dictionary<string, Type> typeMappings, string json)
        {
            var tokenizer = new JSONTokenizer(new StringReader(json));

            var events = new JSONWalkingEvents();

            var constraint = new JSONToSQLConstraint(db, events, typeMappings);

            JSONWalkingValidator walker = new JSONWalkingValidator();
            walker.Walk(tokenizer.GetEnumerator(), events);

            return constraint.GenerateSQLConstraint();
        }
        #endregion

        public SQLConstraint GenerateSQLConstraint()
        {
            if (null != _constraint)
            {
                return _GenerateSQL(_constraint);
            }
            else
            {
                return null;
            }
        }

        #region Private methods

        private SQLConstraint _GenerateSQL(Constraint constraint)
        {
            var sql = new StringBuilder();
            List<IDataParameter> parameters = new List<IDataParameter>();

            _GenerateSQL(sql, parameters, constraint);

            return new SQLConstraint(sql.ToString(), parameters);
        }

        private void _GenerateSQL(StringBuilder sql, List<IDataParameter> parameters, Constraint constraint)
        {
            sql.Append(" " + _db.MakeQuotedName(constraint.Name) + " ");

            if (null == constraint.Value && null == constraint.ArrayValues)
            {
                sql.Append(_operatorsReverse[constraint.Operator]);
                sql.Append(" NULL");
            }
            else
            {
                string paramName = "";
                bool useLike = false;

                if (ConstraintOperator.Like == constraint.Operator)
                {
                    useLike = true;
                }
                else if (ConstraintOperator.NotLike == constraint.Operator)
                {
                    useLike = true;
                    sql.Append("NOT ");
                }
                else
                {
                    sql.Append(_operatorsReverse[constraint.Operator]);
                }

                Type convertToType = null;
                if (_typeMappings.ContainsKey(constraint.Name))
                {
                    convertToType = _typeMappings[constraint.Name];
                }

                if (ConstraintOperator.In == constraint.Operator || ConstraintOperator.NotIn == constraint.Operator)
                {
                    sql.Append(" (");
                    if (null != constraint.ArrayValues && 0 < constraint.ArrayValues.Count)
                    {
                        foreach (var val in constraint.ArrayValues)
                        {
                            paramName = "p_" + parameters.Count;
                            var paramVal = val;
                            if (null != convertToType)
                            {
                                paramVal = Convert.ChangeType(paramVal, convertToType);
                            }
                            parameters.Add(_db.MakeParam(paramName, paramVal));
                            sql.Append(_db.MakeParamReference(paramName));
                            sql.Append(',');
                        }
                        sql.Remove(sql.Length - 1, 1);
                    }
                    else
                    {
                        paramName = "p_" + parameters.Count;
                        var paramVal = constraint.Value;
                        if (null != convertToType)
                        {
                            paramVal = Convert.ChangeType(paramVal, convertToType);
                        }
                        parameters.Add(_db.MakeParam(paramName, paramVal));
                        sql.Append(_db.MakeParamReference(paramName));
                    }
                    sql.Append(")");
                }
                else
                {
                    var paramVal = constraint.Value;
                    if (null != convertToType)
                    {
                        paramVal = Convert.ChangeType(paramVal, convertToType);
                    }
                    paramName = "p_" + parameters.Count;

                    if (useLike)
                    {
                        parameters.Add(ObjectIndexProvider.MakeLikeParameter(_db, paramName, paramVal));
                        sql.Append(_db.MakeLikeParamReference(paramName));
                    }
                    else
                    {
                        parameters.Add(_db.MakeParam(paramName, paramVal));
                        sql.Append(_db.MakeParamReference(paramName));
                    }
                }
            }

            if (null != constraint.Constraints)
            {
                sql.Append(" ");
                sql.Append(constraint.Logic);

                if (1 < constraint.Constraints.Count)
                {
                    sql.Append("(");
                    foreach (var c in constraint.Constraints)
                    {
                        _GenerateSQL(sql, parameters, c);
                        if (ConstraintLogic.NotSet != c.Logic)
                        {
                            sql.Append(' ');
                            sql.Append(c.Logic);
                            sql.Append(' ');
                        }
                    }
                    sql.Append(")");
                }
                else
                {
                    _GenerateSQL(sql, parameters, constraint.Constraints[0]);
                }
            }
        }

        private void _consumeValue(object value)
        {
            if (_constraint.InArray)
            {
                if (_constraintLogic.Contains(_constraint.LastKey))
                {
                    if (0 == _constraint.Constraints.Count)
                    {
                        throw new ArgumentException("Missing constraint near " + value);
                    }
                    else
                    {
                        _constraint.Constraints[_constraint.Constraints.Count - 1].Logic = (ConstraintLogic)Enum.Parse(typeof(ConstraintLogic), value.ToString(), true);
                    }
                }
                else
                {
                    _constraint.ArrayValues.Add(value);
                }
            }
            else
            {
                if (_constraint.InKeyword)
                {
                    switch (_constraint.LastKeyword)
                    {
                        case ConstraintKeywords.Op:
                            if (_operators.ContainsKey(value.ToString()))
                            {
                                _constraint.Operator = _operators[value.ToString()];
                            }
                            else
                            {
                                throw new ArgumentException("Unsupported operator value: " + value);
                            }
                            break;
                        case ConstraintKeywords.And:
                        case ConstraintKeywords.Or:
                        default:
                            throw new ArgumentException("Expecting a new constraint for AND/OR value.");
                    }
                }
                else
                {
                    _constraint.Value = value;
                }
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
            _constraint.InArray = false;
        }

        private void _events_ArrayStart()
        {
            _constraint.InArray = true;
            if (!_constraintLogic.Contains(_constraint.LastKey))
            {
                _constraint.ArrayValues = new List<object>();
            }
        }

        private void _events_ArrayNext()
        {
        }

        private void _events_ObjectKey(string value)
        {
            _constraint.LastKey = value;
            try
            {
                _constraint.LastKeyword = (ConstraintKeywords)Enum.Parse(typeof(ConstraintKeywords), value, true);
                _constraint.InKeyword = true;
            }
            catch
            {
                _constraint.Name = value;
                _constraint.LastKeyword = ConstraintKeywords.Unknown;
                _constraint.InKeyword = false;
            }
        }

        private void _events_ObjectEnd()
        {
            if (0 < _constraints.Count)
            {
                var c = _constraints.Pop();
                c.Constraints.Add(_constraint);
                _constraint = c;
            }
        }

        private void _events_ObjectStart()
        {
            if (null != _constraint)
            {
                if (_constraintLogic.Contains(_constraint.LastKey))
                {
                    _constraint.Logic = (ConstraintLogic)Enum.Parse(typeof(ConstraintLogic), _constraint.LastKey, true);
                    _constraints.Push(_constraint);
                    if (null == _constraint.Constraints)
                    {
                        _constraint.Constraints = new List<Constraint>();
                    }
                }
                else
                {
                    throw new ArgumentException("Invalid constraint syntax. Object literal must follow AND/OR key.");
                }
            }
            _constraint = new Constraint();
        }

        #endregion
    }
}
