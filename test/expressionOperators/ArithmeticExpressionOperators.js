class ArithmeticExpressionOperators {
  static tests = {
    abs: {
      query: "select abs(`Replacement Cost`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $abs: "$Replacement Cost"
              }
            }
          }
        ]
      }
    },

    add: {
      query: "select `Replacement Cost` + 10 + 20 as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $add: [
                {
                  $add: [
                    "$Replacement Cost", 10
                  ]
                },
                20
              ]
            }
          }
        }]
      }
    },

    ceil: {
      query: "select ceiling(`Replacement Cost`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $ceil: "$Replacement Cost"
            }
          }
        }]
      }
    },

    divide: {
      query: "select `Replacement Cost` / 10 / 20 as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $divide: [
                {
                  $divide: [
                    "$Replacement Cost", 10
                  ]
                },
                20
              ]
            }
          }
        }]
      }
    },

    exp: {
      query: "select exp(`Replacement Cost`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $exp: "$Replacement Cost"
            }
          }
        }]
      }
    },

    floor: {
      query: "select floor(`Replacement Cost`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $floor: "$Replacement Cost"
            }
          }
        }]
      }
    },

    ln: {
      query: "select log(`Replacement Cost`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $ln: "$Replacement Cost"
            }
          }
        }]
      }
    },

    log: {
      query: "select log(2, `Replacement Cost`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $log: ["$Replacement Cost", 2]
            }
          }
        }]
      }
    },

    log10: {
      query: "select log10(`Replacement Cost`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $log10: "$Replacement Cost"
            }
          }
        }]
      }
    },

    mod: {
      query: "select mod(`Replacement Cost`, 10) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $mod: ["$Replacement Cost", 10]
            }
          }
        }]
      }
    },

    multiply: {
      query: "select `Replacement Cost` * 10 * 20 as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $multiply: [
                {
                  $multiply: [
                    "$Replacement Cost", 10
                  ]
                },
                20
              ]
            }
          }
        }]
      }
    },

    pow: {
      query: "select pow(abs(`Replacement Cost`), 10) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $pow: ["$Replacement Cost", 10]
            }
          }
        }]
      }
    },

    round: {
      query: "select round(`Replacement Cost`, 2) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $round: ["$Replacement Cost", 2]
            }
          }
        }]
      }
    },

    sqrt: {
      query: "select sqrt(`Replacement Cost`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $sqrt: "$Replacement Cost"
            }
          }
        }]
      }
    },

    subtract: {
      query: "select `Replacement Cost` - 10 - 20 as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $subtract: [
                {
                  $subtract: [
                    "$Replacement Cost", 10
                  ]
                },
                20
              ]
            }
          }
        }]
      }
    },

    trunc: {
      query: "select trunc(`Replacement Cost`, 1) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [{
          $project: {
            aggr: {
              $trunc: ["$Replacement Cost", 1]
            }
          }
        }]
      }
    }

  }
}

module.exports = ArithmeticExpressionOperators;
