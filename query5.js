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
        { $unwind: "$friends" },
        { 
            $project: {
                user_id: 1,
                friend_id: "$friends"
            }
        },
        { 
            $out: "flat_friends"
        }
    ]);

    // Create a collection of friends with their YOB
    db.flat_friends.aggregate([
        {
            $lookup: {
                from: "users",
                localField: "friend_id",
                foreignField: "user_id",
                as: "friend_info"
            }
        },
        { $unwind: "$friend_info" },
        {
            $project: {
                user_id: 1,
                friend_id: 1,
                friend_YOB: "$friend_info.YOB"
            }
        },
        {
            $out: "friend_info"
        }
    ]);

    // Find the oldest friend for each user
    let result = db.friend_info.aggregate([
        {
            $group: {
                _id: "$user_id",
                oldest_friend: {
                    $first: "$friend_id"
                },
                min_yob: { $min: "$friend_YOB" }
            }
        },
        {
            $lookup: {
                from: "friend_info",
                let: { user_id: "$_id", min_yob: "$min_yob" },
                pipeline: [
                    { $match: {
                        $expr: {
                            $and: [
                                { $eq: ["$user_id", "$$user_id"] },
                                { $eq: ["$friend_YOB", "$$min_yob"] }
                            ]
                        }
                    }},
                    { $sort: { friend_id: 1 } },
                    { $limit: 1 }
                ],
                as: "oldest_friend_info"
            }
        },
        { $unwind: "$oldest_friend_info" },
        {
            $project: {
                _id: 0,
                user_id: "$_id",
                oldest_friend: "$oldest_friend_info.friend_id"
            }
        }
    ]).toArray();

    // Convert the result to a JavaScript object
    result.forEach(doc => {
        results[doc.user_id] = doc.oldest_friend;
    });

    return results;
}
