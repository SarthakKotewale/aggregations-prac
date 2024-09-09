use("aggregationPrac");
// use("test");

//challenge01: Find all users who are located in New York.
db.challenges.aggregate([
    {
        $match: {
          "address.city": "New York"
        }
    }
])

// challenge02: Find the user(s) with the email "johndoe@example.com" and retrieve their favorite movie.
db.challenges.aggregate([
    {
        $match: {
            "email": "johndoe@example.com"
        }
    },
    {
        $project: {
            _id: 0,
            "favorite movie": "$favorites.movie"
        }
    }
])

// challenge03: Find all users with "pizza" as their favorite food and sort them according to age.
db.challenges.aggregate([
    {
        $match: {
            "favorites.food": "pizza"
        }
    },
    {
        $sort: {
          age: 1
        }
    }
])

// challenge04: Find all users over 30 whose favorite color is "green".
db.challenges.aggregate([
    {
        $match: {
            age: {
                $gt: 30
            },
            "favorites.color": "green"
        }
    }
])

//challenge05: Count the number of users whose favorite movie is "The Shawshank Redemption."
db.challenges.aggregate([
    {
        $match: {
          "favorites.movie": "The Shawshank Redemption"
        }
    },
    {
        $count: "total"
    }
])

// challenge06: Update the zipcode of the user with the email "johndoe@example.com" to "10002".
db.challenges.aggregate([
    {
        $match: {
            "email": "johndoe@example.com"
        }
    },
    {
        $set: {
          "address.zipcode": "10002"
        }
    }
])

//Group users by their favorite movie and retrieve the average age in each movie group.
db.challenges.aggregate([
    {
      $group: {
        _id: "$favorites.movie",
        averageAge: {
          "$avg": "$age"
        }
      }
    }
  ])

//challenge09: Calculate the average age of users with a favorite " pizza " food.
db.challenges.aggregate([
  {
    $match: {
      "favorites.food": "pizza",
    },
  },
  {
    $group: {
        _id: null,
        averageage: {
            $avg: "$age"
        }
    }
  },
]);

//average age of all users
//count no of males and females

db.users.aggregate([
    {
        $group: {
            _id: "$gender",
            count: {
                $sum: 1
            }
        }
    }
])

//challenge10: Perform a lookup aggregation to retrieve the orders data along with the customer details for each order.
db.orders.aggregate([
    {
        $lookup: {
          from: "customers",
          localField: "customer_id",
          foreignField: "_id",
          as: "inventory",
        }
    },
    {
        $project: { order_number: true, inventory: true },
    },
])

//challenge11: Group users by their favorite color and retrieve the count of users in each color group.
db.challenges.aggregate([
    {
        $group: {
            _id: "$favorites.color",
            userCountColor: {
                $sum: 1
            }
        }
    }
])

//challenge12: Find the user(s) with the highest/Lowest age.
db.challenges.aggregate([
    {
        $sort: {
            age: 1
        }
    },
    {
        $limit: 1
    }
])

// challenge13: Find the most common favorite food among all users.
db.challenges.aggregate([
  {
    $group: {
      _id: "$favorites.food",
      count: {
        $sum: 1,
      },
    },
  },
  {
    $sort: {
      count: -1,
    },
  },
  {
    $limit: 1
  }
]);

//challenge14: Calculate the total count of friends across all users.
db.challenges.aggregate([
    {
        $project: {
            "name": 1,
            yourFriends: {
                $size: "$friends"
            }
        }
    }
])

//challenge15: Find the user(s) with the longest name.
db.challenges.aggregate([
    {
        $addFields: {
            "nameLength": {
                $strLenCP: "$name"
            }
        }
    },
    {
        $sort: {
            "nameLength": -1
        }
    },
    {
        $limit: 1
    }
])

//challenge16 Calculate each state's total number of users in the address field.
db.challenges.aggregate([
    {
        $group: {
            _id: "$address.state",
            count: {
                $sum: 1
            }
        }
    }
])

// challenge17: Find the user(s) with the highest number of friends.

db.challenges.aggregate([
    {
        $project: {
            "name": 1,
            "noOfFriends": {
                $size: "$friends"
            }
        }
    },
    {
        $sort: {
            "noOfFriends": -1
        }
    },
    {
        $limit: 1
    }
])

//aggregate01: Count the number of people in each country.
db.people.aggregate([
    {
        $group: {
            _id: "$address.country",
            count: {
                $sum: 1
            }
        }
    }
])

//aggregate02: What is the most popular address and how many people live there?
db.people.aggregate([
    {
        $group: {
          _id: "$address",
          count: {
            $sum: 1
          }
        }
    },
    {
        $sort: {
            count: -1
        }
    },
    {
        $limit: 1
    }
])

// aggregate03: How many people in each country have ever paid at a restaurant?
db.people.aggregate([
    {
        $match: {
            "payments" : {
                $elemMatch: {
                    "name" : "restaurant"
                }
            }
        }
    },
    {
        $group: {
            _id: "$address.country",
            visits: {
                $sum: 1
            }
        }
    },
    {
        $sort: {
            visits: -1 
        }
    }
])

//aggregate04: Find 3 people with the most total account balance. If a person has the same total asset balance, compare using "firstName" and "lastName" fields. -- Asset balances are stored in the field "wealth.bankAccounts.balance"
db.people.aggregate([
    {
        $unwind: "$wealth.bankAccounts"
    },
    {
        $group: {
            _id: {
                "firstName": "$firstName",
                "lastName": "$lastName"
            },
            totalBalance: {
                $sum: "$wealth.bankAccounts.balance"
            }
        }
    },
    {
        $project: {
            "firstName": 1,
            "lastName": 1,
            "totalBalance": 1
        }
    },
    {
        $limit: 3
    }
])
