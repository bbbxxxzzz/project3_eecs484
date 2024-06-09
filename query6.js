// Query 6
// Find the average friend count per user.
// Return a decimal value as the average user friend count of all users in the users collection.

function find_average_friendcount(dbname) {
    db = db.getSiblingDB(dbname);

    // TODO: calculate the average friend count
    // Step 1: Calculate the total number of friends and the number of users
    let result = db.users.aggregate([
        {
            $project: {
                num_friends: { $size: "$friends" }
            }
        },
        {
            $group: {
                _id: null,
                total_friends: { $sum: "$num_friends" },
                total_users: { $sum: 1 }
            }
        }
    ]).toArray();

    if (result.length === 0) {
        return 0;
    }

    let total_friends = result[0].total_friends;
    let total_users = result[0].total_users;

    // Step 2: Calculate the average friend count
    let average_friendcount = total_friends / total_users;   

    return average_friendcount;
}
