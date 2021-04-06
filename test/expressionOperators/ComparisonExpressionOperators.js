class ComparisonExpressionOperators {
  static tests = {
    cmp: {
      query: "select cmp(`Replacement Cost`, 29) as aggr from `films`",
      output: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $cmp: [
                  "$Replacement Cost", 29
                ]
              }
            }
          }
        ]
      }
    },

    eq: {
      query: "select eq(`Replacement Cost`, 29) as aggr from `films`",
      output: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $eq: [
                  "$Replacement Cost", 29
                ]
              }
            }
          }
        ]
      }
    },

    gt: {
      query: "select gt(`Replacement Cost`, 29) as aggr from `films`",
      output: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $gt: [
                  "$Replacement Cost", 29
                ]
              }
            }
          }
        ]
      }
    },

    gte: {
      query: "select gte(`Replacement Cost`, 29) as aggr from `films`",
      output: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $gte: [
                  "$Replacement Cost", 29
                ]
              }
            }
          }
        ]
      }
    },
    
    lt: {
      query: "select lt(`Replacement Cost`, 29) as aggr from `films`",
      output: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $lt: [
                  "$Replacement Cost", 29
                ]
              }
            }
          }
        ]
      }
    },
    
    lte: {
      query: "select lte(`Replacement Cost`, 29) as aggr from `films`",
      output: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $lte: [
                  "$Replacement Cost", 29
                ]
              }
            }
          }
        ]
      }
    },
    
    ne: {
      query: "select ne(`Replacement Cost`, 29) as aggr from `films`",
      output: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $ne: [
                  "$Replacement Cost", 29
                ]
              }
            }
          }
        ]
      }
    }
  }
}

module.exports = ComparisonExpressionOperators;