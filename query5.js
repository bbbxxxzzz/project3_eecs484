// Query 5
// Find the oldest friend for each user who has a friend. For simplicity,
// use only year of birth to determine age, if there is a tie, use the
// one with smallest user_id. You may find query 2 and query 3 helpful.
// You can create selections if you want. Do not modify users collection.
// Return a javascript object : key is the user_id and the value is the oldest_friend id.
// You should return something like this (order does not matter):
// {user1:userx1, user2:userx2, user3:userx3,...}

function oldest_friend(dbname) {
    db = db.getSiblingDB(dbname);

    let results = {};
    // TODO: implement oldest friends
    db.users.aggregate([
        {$project: {
            user_id: 1,
            friends: 1,
            _id: 0
        }},
        
        {$unwind: "$friends"},

        {$out: "flat_friends_temp"}
    ]);

    // Create inverse relationships
    db.flat_friends_temp.aggregate([
        { $project: {
            user_id: "$friends",
            friends: "$user_id"
        }},
        { $out: "flat_friends_inverse" }
    ]);

    // Combine original and inverse relationships
    db.flat_friends_temp.aggregate([
        { $unionWith: { coll: "flat_friends_inverse" } },
        { $out: "flat_friends_combined" }
    ]);

    db.flat_friends_combined.aggregate([

        {
            $lookup: {
                from: "users",
                localField: "friends",
                foreignField: "user_id",
                as: "friend_info"
            }
        },
        { $unwind: "$friend_info" },
        {
            $project: {
                user_id: 1,
                friend_id: "$friends",
                friend_YOB: "$friend_info.YOB"
            }
        },
        {
            $sort: {
                user_id: 1,
                friend_YOB: 1,
                friend_id: 1
            }
        },
        {
            $group: {
                _id: "$user_id",
                oldest_friend: { $first: "$friend_id" }
            }
        },
        {
            $project: {
                _id: 0,
                user_id: "$_id",
                oldest_friend: 1
            }
        }
    ]).forEach(result => {
        results[result.user_id] = result.oldest_friend;
    });

    // Clean up temporary collections
    db.flat_friends_temp.drop();
    db.flat_friends_inverse.drop();
    db.flat_friends_combined.drop();

    
    return results;
}
