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
 * @fileOverview Contains the ZeroG.YUI global object and helper functions that 
 * utilize YUI v2.
 * @name ZeroG.YUI.js
 * @author Jeremy Burman jeremy.burman@zerogsoftware.com
 */
 
if('undefined' !== typeof (ZeroG) && ZeroG) {
    if ('undefined' === typeof (ZeroG.YUI) || !ZeroG.YUI) {
        /**
        * The root ZeroG.YUI object, which holds helper functions that utilize YUI
        * @class
        */
        ZeroG.YUI = {
            /**
            * Creates a queue based on ZeroG.createQueue but augments it with a 
            * queueEmptyEvent (YUI CustomEvent) that fires whenever items are removed 
            * from the queue and it becomes empty.
            * @function
            */
            createQueue: function() {
                var queue = ZeroG.createQueue();
                
                var dequeueFromProto = queue.dequeue;
                
                // replace the dequeue function from the queue's prototype.
                queue.dequeue = function() {
                    var returnVal = dequeueFromProto();
                    if(0 === queue.length()) {
                        queue.queueEmptyEvent.fire();
                    }
                    return returnVal;
                };
                
                queue.queueEmptyEvent = new YAHOO.util.CustomEvent('queueEmptyEvent', queue);
                
                return queue;
            },
            findParentWithClass: function(el, className) {
                var parentNode = null;
                if(el && (parentNode = el.parentNode)) {
                    if(YAHOO.util.Dom.hasClass(parentNode, className)) {
                        return parentNode;
                    } else {
                        return ZeroG.YUI.findParentWithClass(parentNode, className);
                    }
                }
            },
            /**
            * Finds the first child element under a given root node that has a given class name 
            * and is of a given tag type.
            * @param {String} [className] The class name to search for.
            * @param {String} [tagName] The type of tag to search for.
            * @param {HTMLElement} [rootEl] The root node to search under.  If null or left undefined then document.body is used.
            * @returns {HTMLElement} The first HTML Element found with the given class name and tag name, or null if none is found.
            * @function
            */
            getElByClassName: function(className, tagName, rootEl) {
                var searchEl = (rootEl)?rootEl:document.body;
                var els = YAHOO.util.Dom.getElementsByClassName(className, tagName, searchEl);
                if(els && 0 < els.length) {
                    return els[0];
                } else {
                    return null;
                }
            },
            resizeV: function(elId, marginBot, minHeight, freq) {
                var el = YAHOO.util.Dom.get(elId);
                if (el && el.style.display !== 'none') {
                    var vpHeight = YAHOO.util.Dom.getViewportHeight();
                    var newHeight = (vpHeight - (el.offsetTop + marginBot));
            
                    if (newHeight < minHeight) {
                        newHeight = minHeight;
                    }
                    newHeight += 'px';
                    if (el.style.height !== newHeight) {
                        el.style.height = newHeight;
                    }
                }
                if (ZeroG.isDefined(freq)) {
                    setTimeout(function() { ZeroG.resizeV(elId, marginBot, minHeight, freq); }, freq);
                }
            }
        };
    }
}
