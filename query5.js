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
            _id: 0, 
            user_id: 1,
            friends: 1
        }},
        
        {$unwind: "$friends"},

        {$out: "flat_friends_temp"}
    ]);

    // Create inverse relationships
    db.flat_friends_temp.aggregate([
        { $project: {
            _id: 0,
            user_id: "$friends",
            friends: "$user_id"
        }},
        { $out: "flat_friends_inverse" }
    ]);

    // Retrieve original and inverse relationships and merge them in the application code
    let original = db.flat_friends_temp.find().toArray();
    let inverse = db.flat_friends_inverse.find().toArray();

    // Combine the results
    let combined = original.concat(inverse);

    // Insert combined results into the final collection
    db.flat_friends_combined.insertMany(combined);

    // Clean up temporary collections
    db.flat_friends_temp.drop();
    db.flat_friends_inverse.drop();

    db.flat_friends_combined.aggregate([

        {
            $lookup: {
                from: "users",
                localField: "$friends",
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

    db.flat_friends_combined.drop();

    
    return results;
}
