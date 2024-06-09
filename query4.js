// Query 4
// Find user pairs (A,B) that meet the following constraints:
// i) user A is male and user B is female
// ii) their Year_Of_Birth difference is less than year_diff
// iii) user A and B are not friends
// iv) user A and B are from the same hometown city
// The following is the schema for output pairs:
// [
//      [user_id1, user_id2],
//      [user_id1, user_id3],
//      [user_id4, user_id2],
//      ...
//  ]
// user_id is the field from the users collection. Do not use the _id field in users.
// Return an array of arrays.

function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname);

    let pairs = [];
    // TODO: implement suggest friends
    // Retrieve all male and female users
    let males = db.users.find({ gender: "male" }).toArray();
    let females = db.users.find({ gender: "female" }).toArray();

    // Iterate over each male user
    males.forEach(male => {
        // Iterate over each female user
        females.forEach(female => {
            // Check if they meet the conditions
            if (Math.abs(male.YOB - female.YOB) < year_diff &&
                male.hometown.city === female.hometown.city &&
                (male.friends.indexOf(female.user_id) === -1 && female.friends.indexOf(male.user_id) === -1)) {
                
                // Add the pair to the results
                pairs.push([male.user_id, female.user_id]);
            }
        });
    });

    return pairs;
}
