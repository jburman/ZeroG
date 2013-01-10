using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Runtime.Serialization;
using ProtoBuf;
using ZeroG.Data.Object;

namespace ZeroGDemoWeb
{
    public enum ActivityType : int
    {
        News,
        Article
    }

    /// <summary>
    /// The data model for Activities
    /// </summary>
    [DataContract]
    public class Activity
    {
        [DataMember(Order = 1)]
        public int ID { get; set; }
        [Required]
        [MaxLength(100)]
        [DataMember(Order = 2)]
        public string Title { get; set; }
        [DataMember(Order = 3)]
        public string Link { get; set; }
        [DataMember(Order = 4)]
        public ActivityType ActivityType { get; set; }
        [MaxLength(1000)]
        [DataMember(Order = 5)]
        public string Text { get; set; }
        [DataMember(Order = 6)]
        public string Image { get; set; }
        [DataMember(Order = 7)]
        public DateTime CreatedUTC { get; set; }
        [DataMember(Order = 8)]
        public DateTime ExpiresUTC { get; set; }

        public static byte[] Serialize(Activity act)
        {
            var buffer = new MemoryStream();
            Serializer.Serialize<Activity>(buffer, act);
            return buffer.ToArray();
        }

        public static Activity Deserialize(byte[] data)
        {
            return Serializer.Deserialize<Activity>(new MemoryStream(data));
        }
    }

    /// <summary>
    /// This class demonstrates using the ZeroG Object Service to implement an "Activity Feed".
    /// Activities can be added with an Expiration date and then returned in reverse chronological order.
    /// </summary>
    public class ActivityFeed
    {
        /// <summary>
        /// Name of the Object Store to store Activities in.
        /// </summary>
        public const string ObjectName = "ActivityFeed";

        /// <summary>
        /// Name of the Object Index for Activity expiration values.
        /// </summary>
        public const string IndexExpires = "Expires";
        /// <summary>
        /// Name of the Object Index for Activity type values.
        /// </summary>
        public const string IndexType = "Type";

        /// <summary>
        /// Creates the Object Store metadata needed by the Object Service to build the Activity Feed's 
        /// object store.
        /// </summary>
        /// <returns></returns>
        public static ObjectMetadata GetMetadata()
        {
            return new ObjectMetadata(Globals.ObjectNameSpace,
                ObjectName,
                new ObjectIndexMetadata[]
                {
                    new ObjectIndexMetadata(IndexExpires, ObjectIndexType.DateTime, 0, true),
                    new ObjectIndexMetadata(IndexType, ObjectIndexType.Integer)
                });
        }

        /// <summary>
        /// The Object Service client object that is used to interact with the Object Store.
        /// </summary>
        private IObjectServiceClient _svc;

        public ActivityFeed(ObjectService svc)
        {
            // LocalObjectServiceClient means that hte ObjectService is running on the local machine
            _svc = new LocalObjectServiceClient(svc, Globals.ObjectNameSpace, ObjectName);
        }

        /// <summary>
        /// Adds a new Activity to the Object Store. A new Object ID will be generated and assigned to 
        /// the Activity instance.
        /// </summary>
        /// <param name="activity"></param>
        /// <returns></returns>
        public ObjectID Add(Activity activity)
        {
            ObjectID returnValue = null;

            if (null != activity)
            {
                activity.ID = _svc.NextID();

                returnValue = _svc.Store(
                    activity.ID,
                    null,
                    Activity.Serialize(activity),
                    new ObjectIndex[]
                {
                    ObjectIndex.Create(IndexType, (int)activity.ActivityType),
                    ObjectIndex.Create(IndexExpires, activity.ExpiresUTC),
                });
            }

            return returnValue;
        }

        /// <summary>
        /// Lookup an Activity by its ID.
        /// </summary>
        /// <param name="activityId"></param>
        /// <returns></returns>
        public Activity Get(int activityId)
        {
            Activity returnValue = null;

            var val = _svc.Get(activityId);
            if (null != val)
            {
                returnValue = Activity.Deserialize(val);
            }

            return returnValue;
        }

        /// <summary>
        /// Remove an Activity by its ID.
        /// </summary>
        /// <param name="activityId"></param>
        public void Remove(int activityId)
        {
            _svc.Remove(activityId);
        }

        /// <summary>
        /// Overwrite an existing Activity object.
        /// </summary>
        /// <param name="activityId"></param>
        /// <param name="activity"></param>
        public void Update(int activityId, Activity activity)
        {
            if (null != activity)
            {
                if (activity.ID != activityId)
                {
                    activity.ID = activityId;
                }

                _svc.Store(activityId,
                    null,
                    Activity.Serialize(activity),
                    new ObjectIndex[]
                {
                    ObjectIndex.Create(IndexType, (int)activity.ActivityType),
                    ObjectIndex.Create(IndexExpires, activity.ExpiresUTC),
                });
            }
        }

        /// <summary>
        /// Returns a given number of recent Activity.
        /// </summary>
        /// <param name="count">How many Activity items to return.</param>
        /// <returns></returns>
        public IEnumerable<Activity> Recent(uint count)
        {
            var result = _svc.Find(new ObjectFindOptions()
            {
                Limit = count,
                Order = new OrderOptions()
                {
                    Descending = true,
                    Indexes = new string[] { IndexExpires }
                }
            }, null);

            foreach (var val in result)
            {
                yield return Activity.Deserialize(val);
            }
        }

        /// <summary>
        /// Returns a given number of recent Activity items of a specific type.
        /// </summary>
        /// <param name="count"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public IEnumerable<Activity> RecentOfType(uint count, ActivityType type)
        {
            var result = _svc.Find(new ObjectFindOptions()
            {
                Limit = count,
                Order = new OrderOptions()
                {
                    Descending = true,
                    Indexes = new string[] { IndexExpires }
                }
            },
            new ObjectIndex[] { ObjectIndex.Create(IndexType, (int)type) });

            foreach (var val in result)
            {
                yield return Activity.Deserialize(val);
            }
        }
    }
}