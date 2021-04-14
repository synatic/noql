class BooleanExpressionOperators {
  static tests = {
    and: {
      query: "select and(true, 0) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $and: [
                  true, 0
                ]
              }
            }
          }
        ]
      }
    },

    not: {
      query: "select not(true, 0) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $not: [
                  true, 0
                ]
              }
            }
          }
        ]
      }
    },

    or: {
      query: "select or(true, 0) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $or: [
                  true, 0
                ]
              }
            }
          }
        ]
      }
    }

  }
}

module.exports = BooleanExpressionOperators;
