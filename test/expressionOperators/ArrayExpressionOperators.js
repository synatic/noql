class ArrayExpressionOperators {
  static tests = {
    arrayElemAt: {
      query: "select arrayElemAt(`Actors`, 0) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $arrayElemAt: [
                  "$Actors", 0
                ]
              }
            }
          }
        ]
      }
    },

    arrayToObject: {
      query: "select arrayToObject(`Actors`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $arrayToObject: "$Actors"
              }
            }
          }
        ]
      }
    },

    concatArrays: {
      query: "select concatArrays(`Actors`, `Actors`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $concatArrays: [
                  "$Actors", "$Actors"
                ]
              }
            }
          }
        ]
      }
    },

    filter: {
      query: "select filter(`Actors`, `Acts`, ) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $filter: [
                  "$Actors", "$Actors"
                ]
              }
            }
          }
        ]
      }
    },

    first: {
      query: "select arrayFirst(`Actors`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $first: "$Actors"
              }
            }
          }
        ]
      },
        queryOutput: {
            collection: "films",
            limit:100,
            projection: {
                aggr: {
                    $first: "$Actors"
                }

            }
        }
    },

    in: {
      query: "select in(123, `Actors`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $in: [
                  123, "$Actors"
                ]
              }
            }
          }
        ]
      }
    },

    indexOfArray: {
      query: "select indexOfArray(`Actors`, 123) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $indexOfArray: [
                  "$Actors", 123
                ]
              }
            }
          }
        ]
      }
    },

    isArray: {
      query: "select isArray(`Actors`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $isArray: "$Actors"
              }
            }
          }
        ]
      }
    },

    last: {
      query: "select last(`Actors`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $last: "$Actors"
              }
            }
          }
        ]
      }
    },

    map: {
      query: "select map(`Actors`, ) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $map: "$Actors"
              }
            }
          }
        ]
      }
    },

    objectToArray: {
      query: "select objectToArray(`Address`) as aggr from `customers`",
      aggregateOutput: {
        collections: ["customers"],
        pipeline: [
          {
            $project: {
              aggr: {
                $objectToArray: "$Address"
              }
            }
          }
        ]
      }
    },

    range: {
      query: "select range(`Address`, ) as aggr from `customers`",
      aggregateOutput: {
        collections: ["customers"],
        pipeline: [
          {
            $project: {
              aggr: {
                $range: "$Address"
              }
            }
          }
        ]
      }
    },

    reduce: {
      query: "select reduce(`Address`, ) as aggr from `customers`",
      aggregateOutput: {
        collections: ["customers"],
        pipeline: [
          {
            $project: {
              aggr: {
                $reduce: "$Address"
              }
            }
          }
        ]
      }
    },

    reverseArray: {
      query: "select reverseArray(`Actors`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $reverseArray: "$Actors"
              }
            }
          }
        ]
      }
    },

    size: {
      query: "select arrayLength(`Actors`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $size: "$Actors"
              }
            }
          }
        ]
      }
    },

    slice: {
      query: "select arraySlice(`Actors`, 2) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $slice: [
                  "$Actors", 2
                ]
              }
            }
          }
        ]
      }
    },

    zip: {
      query: "select arrayZip(`Actors`, `Actors`) as aggr from `films`",
      aggregateOutput: {
        collections: ["films"],
        pipeline: [
          {
            $project: {
              aggr: {
                $zip: [
                  "$Actors", "$Actors"
                ]
              }
            }
          }
        ]
      }
    }
  }
}

module.exports = ArrayExpressionOperators;
