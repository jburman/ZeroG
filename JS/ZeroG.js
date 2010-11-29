/*
Copyright (c) 2010 Jeremy Burman

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.
*/
/**
 * @fileOverview Contains the ZeroG global object and global String enhancements.
 * @name ZeroG.js
 * @author Jeremy Burman jeremy.burman@zerogsoftware.com
 */

/**
* Makes a string safe for HTML.
* @function
* @augments String
* @author Doug Crockford http://javascript.crockford.com/remedial.html
*/
String.prototype.entityify = function () {
    return this.replace(/&/g, "&amp;").replace(/</g,
        "&lt;").replace(/>/g, "&gt;");
};


/**
* Quotes a string and escapes any backslashes.
* @function
* @augments String
* @author Doug Crockford http://javascript.crockford.com/remedial.html
*/
String.prototype.quote = function () {
    var c, i, l = this.length, o = '"';
    for (i = 0; i < l; i += 1) {
        c = this.charAt(i);
        if (c >= ' ') {
            if (c === '\\' || c === '"') {
                o += '\\';
            }
            o += c;
        } else {
            switch (c) {
            case '\b':
                o += '\\b';
                break;
            case '\f':
                o += '\\f';
                break;
            case '\n':
                o += '\\n';
                break;
            case '\r':
                o += '\\r';
                break;
            case '\t':
                o += '\\t';
                break;
            default:
                c = c.charCodeAt(0);
                o += '\\u00' + Math.floor(c / 16).toString(16) +
                    (c % 16).toString(16);
            }
        }
    }
    return o + '"';
};

/**
* Replaces keywords in a string that are wrapped in {} with corresponding 
* values from the supplied object where the object's keys match the keywords.
* @function
* @augments String
* @param {Object} [o] An object to use to supplant keywords in the string with.
* @author Doug Crockford http://javascript.crockford.com/remedial.html
*/
String.prototype.supplant = function (o) {
    return this.replace(/{([^{}]*)}/g,
        function (a, b) {
            var r = o[b];
            return typeof r === 'string' || typeof r === 'number' ? r : a;
        }
    );
};

/**
* Trims whitespace off of a string.
* @function
* @augments String
* @author Doug Crockford http://javascript.crockford.com/remedial.html
*/
String.prototype.trim = function () {
    return this.replace(/^\s+|\s+$/g, "");
};

if (typeof Object.create !== 'function') {
    Object.create = function (o) {
        function F() {}
        F.prototype = o;
        return new F();
    };
}

if ('undefined' === typeof (ZeroG) || !ZeroG) {
    /**
    * The root ZeroG object, which holds many helper utility functions.
    * @class
    */
    ZeroG = {
        /**
        * Applies a function to an array of objects.  Each array element 
        * is passed to the supplied function parameter in order.
        * @function apply
        * @param {Array} [array] The array to apply the function to.
        * @param {Function} [fn] The function to call on each array element.
        */
        apply: function(array, fn) {
            if (array && fn) {
                var len = array.length;
                for (var i = 0; len > i; i++) {
                    fn(array[i]);
                }
            }
        },
        /**
        * Converts a JavaScript object's properties and values into a query string.  Each value 
        * is urlEncoded.
        * @function buildQueryString
        * @param {Object} [args] The object whose properties and values will be converted into a query string.
        */
        buildQueryString: function(args) {
            var qstring = [];
            if(args) {
                for(var argKey in args) {
                    if(args.hasOwnProperty(argKey)) {
                        qstring.push('&' + argKey + '=' + ZeroG.urlEncode(args[argKey]));
                    }
                }
            }
            return qstring.join('');
        },
        /**
        * "Commafies" a numeric value.  I.e. use this method to turn 30000 into 30,000.
        * @function buildQueryString
        * @param {Number} [numVal] The numeric value to commafy.
        * @param {Number} [separation] The maximum number of characters between each comma (going right to left). 
        */
        commafy: function(numVal, separation) {
            if(null !== numVal) {
                var valStr = numVal.toString();
                var len = valStr.length;
                var dotdex = valStr.indexOf('.');
                var i = len-1;
                var remainder = '';
                var count = 1;
                var arr = [];
                if(-1 !== dotdex) {
                    i = dotdex - 1;
                    remainder = valStr.substr(dotdex, len - dotdex);
                }
                for(; -1 < i; --i,++count) {
                    arr.push(valStr.charAt(i));
                    if(1 !== count && 0 !== i && 0 === (count % separation)) {
                        arr.push(',');
                    }            
                }
                arr.reverse();
                return arr.join('') + remainder;
            } else {
                return numVal;
            }
        },
        /**
        * Entityifies a string for inclusing in an HTML document.
        * This method checks if the supplied argument is a value and converts
        * it to a string and entityifies it if so.
        * @function
        * @param {Object} [val] The value to entityify.
        */
        cook4HTML: function(val) {
            if (!ZeroG.isNullOrEmpty(val)) {
                return val.toString().entityify();
            }
            return val;
        },
        createRow: function(height, colWidths) {
            var row = {};
            var cols = [];
            var rowEl = document.createElement('div');
            var el = null;
            var len = colWidths.length;
        
            rowEl.style.height = height.toString() + 'px';
            for (var i = 0; len > i; i++) {
                el = document.createElement('div');
        
                el.style.width = colWidths[i].toString() + 'px';
        
                ZeroG.setCSSFloat(el, 'left');
        
                rowEl.appendChild(el);
                cols.push(el);
            }
        
            row.row = rowEl;
            row.cols = cols;
        
            return row;
        },
        /**
        * Creates an object that acts as a queue.  It has the following methods.
        * <ul>
        *   <li>enqueue(item) - Adds a new item to the end of the queue.</li>
        *   <li>dequeue() - Removes the next item from the front of the queue.</li>
        *   <li>length() - Returns the length of the queue.</li>
        *   <li>peek() - Returns the next item at the front of the queue without removing it.</li>
        * </ul>
        * @function
        */
        createQueue: function() {
            // private data field
            var data = [];
            var fnLength = function() {
                return data.length;
            };
            
            var queue = {
                enqueue: function(item) {
                    data.push(item);
                },
                dequeue: function() {
                    if(0 < fnLength()) {
                        return data.shift();
                    }
                },
                length: fnLength,
                peek: function() {
                    if(0 < fnLength()) {
                        return data[0];
                    }
                }
            };
            
            return Object.create(queue);
        },
        createTableRow: function (height, colWidths) {
            var table = document.createElement('table');
            table.border = 0;
            table.cellPadding = table.cellSpacing = 0;
            table.style.height = height.toString() + 'px';
            table.style.border = 'none';
        
            var totalWidth = 0;
            var rowEl = document.createElement('tr');
            table.appendChild(rowEl);
        
            var cols = [];
            var el = null;
        
            var len = colWidths.length;
            for (var i = 0; len > i; i++) {
                el = document.createElement('td');
                el.style.border = 'none';
        
                el.style.height = height.toString() + 'px';
                el.style.width = colWidths[i].toString() + 'px';
        
                cols.push(el);
                rowEl.appendChild(el);
        
                totalWidth += colWidths[i];
            }
        
            table.row = table;
            table.cols = cols;
            table.width = totalWidth.toString() + 'px';
        
            return table;
        },
        findInArray: function(haystack, fn) {
            if (haystack && fn) {
                var len = haystack.length;
                for (var i = 0; len > i; i++) {
                    var val = fn(haystack[i]);
                    if (null !== val) {
                        return val;
                    }
                }
            }
            return null;
        },
        handleKeyEvent: function(evt) {
            var key = null;
            if (evt) {
                if (evt.keyCode) {
                    key = evt.keyCode;
                    evt.cancelBubble = true;
                }
                else {
                    key = evt.which;
                }
            }
            else {
                key = window.event.keyCode;
            }
            return key;
        },
        hideEl: function(el) {
            if(typeof(el) === 'string') {
                el = document.getElementById(el);
            }
            if(el) {
                el.style.display = 'none';
            }
        },
        isVisible: function(el) {
            return(ZeroG.isDefined(el) && null !== el && el.style.display !== 'none');
        },
        showEl: function(el) {
            if(typeof(el) === 'string') {
                el = document.getElementById(el);
            }
            if(el) {
                el.style.display = '';
            }
        },
        IEVer: function() {
            var rv = -1;
            if (navigator.appName == 'Microsoft Internet Explorer') {
                var ua = navigator.userAgent;
                var re = new RegExp("MSIE ([0-9]{1,}[\\.0-9]{0,})");
                if (re.exec(ua) !== null) {
                    rv = parseFloat(RegExp.$1);
                }
            }
            return rv;
        },
        /**
        * Determines if an object contains any enumerable key/values.
        * @function
        * @param {Object} [o] The value to determine if empty.
        * @author Doug Crockford http://javascript.crockford.com/remedial.html
        */
        isEmpty: function(o) {
            var i, v;
            if (typeOf(o) === 'object') {
                for (i in o) {
                    if(o.hasOwnProperty(i)) {
                        v = o[i];
                        if (v !== undefined && typeOf(v) !== 'function') {
                            return false;
                        }
                    }
                }
            }
            return true;
        },
        isDefined: function(val) {
            return ('undefined' !== typeof (val));
        },
        isNullOrEmpty: function(val) {
            if (null === val || '' === val) {
                return true;
            } else {
                return false;
            }
        },
        /**
        * Determines if a DOM node is visible and enabled.
        * @function
        * @param {HTMLElement} [el] The element to test.
        */
        isUsable: function(el) {
            return (null !== el && false === el.disabled && 'none' !== el.style.display);
        },
        /**
        * Parses date strings in the Microsoft AJAX format.  Constructs a new JavaScript Date
        * and adjusts it to the local time.
        * Example: /Date(1286403877000-0400)/
        * @param [String] {dataStr} The date time string to parse.
        */
        parseMSAjaxDate: function(dateStr) {
            if(!ZeroG.isNullOrEmpty(dateStr)) {
                // extract the Milliseconds portion and construct a date
                var dateMatch = dateStr.match(/([0-9]+)(\+|\-)([0-9]+)/);
                if(4 === dateMatch.length) {
                    var millis = +dateMatch[1];
                    var sign = dateMatch[2];
                    var offset = +dateMatch[3];
                    
                    var offsetMillis = (offset / 100) * 60 * 60 * 1000;
                    
                    if(sign === '-') {
                        offsetMillis *= -1; 
                    }
                    return new Date(millis + offsetMillis);        
                }
            }
            return null;
        },
        postDataEncode: function(val) {
            if (!ZeroG.isNullOrEmpty(val)) {
                val = val.toString();
                val = val.replace(/&/g, '%26');
                val = val.replace(/\+/g, '%2B');
            }
            return val;
        },
        replace: function(text, findVal, replaceVal) {
            if(!ZeroG.isNullOrEmpty(text) && !ZeroG.isNullOrEmpty(findVal) && null !== replaceVal) {
                return text.replace(findVal, replaceVal);
            } else {
                return text;
            }
        },
        scrollTop: function() {
            document.body.scrollIntoView(true);
        },
        scrollToEl: function(el) {
            if(el) {
                if('string' === typeof(el)) {
                    el = document.getElementById(el);
                }
                el.scrollIntoView(true);
            }
        },
        setCSSFloat: function(el, style) {
            if(ZeroG.isDefined(el.style.cssFloat)) {
                el.style.cssFloat = style;
            } else {
                el.style.styleFloat = style;
            }
        },
        setLocationTarget: function(target) {
            window.location.hash = '#' + target;
        },
        toShortDateString: function(date) {
            if(date) {
                var month = date.getMonth()+1;
                var day = date.getDate();
                if(day < 10) { day = '0' + day; }
                return (month + '/' + day + '/' + date.getFullYear());
            } else { 
                return ''; 
            }
        },
        toTimeString: function(date) {
            if(date) {
                var hours = date.getHours();
                var minutes = date.getMinutes();
                var isPM = hours > 11;
                
                hours = hours % 12;
                if(0 === hours) {
                    hours = 12;
                }
                
                if(minutes < 10) { minutes = '0' + minutes; }
                return (hours + ':' + minutes + ' ' + (isPM?'PM':'AM'));
            } else { 
                return ''; 
            }
        },
        urlEncode: function(val) {
            if (!ZeroG.isNullOrEmpty(val)) {
                val = val.toString();
                val = val.replace(/%/g, '%25');
                val = val.replace(/#/g, '%23');
                val = val.replace(/&/g, '%26');
                val = val.replace(/\+/g, '%2B');
            }
            return val;
        },
        /**
        * Provides a better implementation for typeOf (where the built in typeof will return 
        * 'object' for both arrays and null values, this implementation will return 'array' and 
        * 'null' respectively).
        * @function
        * @param {Object} [value] The value to determine the type of.
        * @author Doug Crockford http://javascript.crockford.com/remedial.html
        */
        typeOf: function(value) {
            var s = typeof value;
            if (s === 'object') {
                if (value) {
                    if (typeof value.length === 'number' &&
                            !(value.propertyIsEnumerable('length')) &&
                            typeof value.splice === 'function') {
                        s = 'array';
                    }
                } else {
                    s = 'null';
                }
            }
            return s;
        },
        countProperties: function (obj) {
            var count = 0;
            for (k in obj) {
                if (obj.hasOwnProperty(k)) {
                    count++;
                }
            }
            return count;
        }
    };
    
    /**
    * @class
    */
    ZeroG.Cookies = {
        /**
        * @author Peter-Paul Koch http://www.quirksmode.org/js/cookies.html
        * @function
        */
        createCookie: function(name,value,days) {
            var expires, date;
            
            if (days) {
                date = new Date();
                date.setTime(date.getTime()+(days*24*60*60*1000));
                expires = "; expires="+date.toGMTString();
            } else {
                expires = "";
            }
            document.cookie = name+"="+value+expires+"; path=/";
        },
        /**
        * @author Peter-Paul Koch http://www.quirksmode.org/js/cookies.html
        * @function
        */
        readCookie: function(name) {
            var nameEQ = name + "=";
            var ca = document.cookie.split(';');
            for(var i=0;i < ca.length;i++) {
                var c = ca[i];
                while (c.charAt(0)==' ') {
                    c = c.substring(1,c.length);
                }
                if (c.indexOf(nameEQ) === 0) { 
                    return c.substring(nameEQ.length,c.length);
                }
            }
            return null;
        },
        /**
        * @author Peter-Paul Koch http://www.quirksmode.org/js/cookies.html
        * @function
        */
        eraseCookie: function(name) {
            ZeroG.Cookies.createCookie(name,"",-1);
        }
    };
}
