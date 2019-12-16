var app = require("express")();
var bodyParser = require("body-parser");

let AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });

let _tableName = process.env.TABLE_NAME;

let playerDatabase = require("./model.js");

function jsonContentType(req, res) {
  const content_type = req.get("Content-Type");
  if (content_type !== "application/json") {
    res.status(400).send("Invalid Content-Type. Please use 'application/json'");
    return false;
  }
  return true;
}

/* ---------- BigQuery Authentication Credentials ---------- */


/* ---------- Service info calls ---------- */
var details = {};
try {
  details = require("../package.json");
} catch (e) {
  console.error("Could not load 'package.json'.");
}

app.get("/info", function(req, res) {
  res.status(200).json({
    name: details.name,
    version: details.version
  });
});

app.get("/info/health", function(req, res) {
  res.sendStatus(200);
});

/* ---------- Endpoints PRODUCTION ---------- */

app.post("/getMessages", function(req, res) {
  if (!jsonContentType(req, res)) {
    return;
  }

  const playerEmail = req.body.email;
  var params = {
    TableName: _tableName,
    Key: { email: playerEmail }
  };

  let client = new AWS.DynamoDB.DocumentClient();
  client.get(params, function(err, data) {
    if (err) {
      let errString = "Unable to find player. Error JSON: " + JSON.stringify(err, null, 2);
      console.log(errString);
      res.status(400).send(errString);
    } else {
      res.status(200).send(data.Item.messages);
    }
  });
});

app.post("/getPlayerDetails", function(req, res) {
  if (!jsonContentType(req, res)) {
    return;
  }

  playerDatabase.getPlayerDetails(req, res);
  // const playerEmail = req.body.email;
  // var params = {
  //   TableName: _tableName,
  //   Key: { email: playerEmail }
  // };

  // let client = new AWS.DynamoDB.DocumentClient();
  // client.get(params, function(err, data) {
  //   if (err) {
  //     let errString = "Unable to find player. Error JSON: " + JSON.stringify(err, null, 2);
  //     console.log(errString);
  //     res.status(400).send(errString);
  //   } else {
  //     res.status(200).send(data.Item);
  //   }
  // });
});

app.post("/getPlayerWithEmail", function(req, res) {
  if (!jsonContentType(req, res)) {
    return;
  }

  const playerEmailPortion = req.body.email;
  var params = {
    TableName: _tableName,
    FilterExpression: "contains(email, :email)",
    ExpressionAttributeValues: {
      ":email": playerEmailPortion
    },
    ProjectionExpression: "email, bspotId, dateOfCreation, adwStatus"
  };

  let client = new AWS.DynamoDB.DocumentClient();
  client.scan(params, function(err, data) {
    if (err) {
      let errString = "Unable to find players. Error JSON: " + JSON.stringify(err, null, 2);
      console.log(errString);
      res.status(400).send(errString);
    } else {
      res.status(200).send(data.Items);
    }
  });
});

app.post("/getAllPlayers", function(req, res) {
  if (!jsonContentType(req, res)) {
    return;
  }

  var params = {
    TableName: _tableName,
    ProjectionExpression: "email, bspotId, dateOfCreation, adwStatus"
  };

  let client = new AWS.DynamoDB.DocumentClient();
  client.scan(params, function(err, data) {
    if (err) {
      let errString = "Unable to find players. Error JSON: " + JSON.stringify(err, null, 2);
      console.log(errString);
      res.status(400).send(errString);
    } else {
      res.status(200).send(data.Items);
    }
  });
});

app.post("/getPlayersSinceDate", function(req, res) {
  if (!jsonContentType(req, res)) {
    return;
  }

  const date = req.body.date;
  console.log("Attempting to find players added since: " + date);
  var params = {
    TableName: _tableName,
    FilterExpression: "dateOfCreation > :num",
    ExpressionAttributeValues: { ":num": date },
    ProjectionExpression: "email, bspotId, dateOfCreation, adwStatus"
  };

  let client = new AWS.DynamoDB.DocumentClient();
  client.scan(params, function(err, data) {
    if (err) {
      let errString = "Unable to find players. Error JSON: " + JSON.stringify(err, null, 2);
      console.log(errString);
      res.status(400).send(errString);
    } else {
      console.log("Found " + data.Items.length + " players");
      res.status(200).send(data.Items);
    }
  });
});

app.post("/getPlayersBetweenDates", function(req, res) {
  if (!jsonContentType(req, res)) {
    return;
  }

  const startDate = req.body.fromDate;
  const endDate = req.body.toDate;
  console.log("Attempting to find players added between: " + startDate + " and: " + endDate);
  var params = {
    TableName: _tableName,
    FilterExpression: "dateOfCreation >= :fromDateTime AND dateOfCreation <= :toDateTime",
    ProjectionExpression: "email, bspotId, dateOfCreation, adwStatus",
    ExpressionAttributeValues: {
      ":fromDateTime": startDate,
      ":toDateTime": endDate
    }
  };
  let client = new AWS.DynamoDB.DocumentClient();
  client.scan(params, function(err, data) {
    if (err) {
      let errString = "Unable to find players. Error JSON: " + JSON.stringify(err, null, 2);
      console.log(errString);
      res.status(400).send(errString);
    } else {
      console.log("Found " + data.Items.length + " players");
      res.status(200).send(data.Items);
    }
  });
});

/* ---------- BigQuery Endpoints ---------- */

app.post("/getTotalWager", function(req, res) {
  async function query() {
    // Queries a public Stack Overflow dataset.
    console.log("authentication successful runnig query");
    // The SQL query to run
    const sqlQuery = `SELECT COUNT(cost)
      FROM wildruby_pwa_prod.wager_success`;

    const options = {
      query: sqlQuery,
      // Location must match that of the dataset(s) referenced in the query.
      location: "US"
    };

    // Run the query
    const [rows] = await bigqueryClient.query(options);

    console.log("Query Results:");
    rows.forEach(row => console.log(row));

    res.status(200).send(rows);
  }
  query();
});

app.post("/getWagers", function(req, res) {
  async function query() {
    const sqlQuery = `SELECT cost, user_id, original_timestamp 
      FROM wildruby_pwa_prod.wager_success`;
    const options = {
      query: sqlQuery,
      location: "US"
    };
    const [rows] = await bigqueryClient.query(options);
    rows.forEach(row => console.log(row));
    res.status(200).send(rows);
  }
  query();
});

app.post("/getUserCount", function(req, res) {
  async function query() {
    const sqlQuery = `SELECT COUNT(DISTINCT user_id)
      FROM wildruby_pwa_prod.wager_success`;
    const options = {
      query: sqlQuery,
      location: "US"
    };
    const [rows] = await bigqueryClient.query(options);
    res.status(200).send(rows);
  }
  query();
});

app.post("/getBetCount", function(req, res) {
  async function query() {
    const sqlQuery = `SELECT (COUNT(amount)/100)
      FROM wildruby_pwa_prod.player_bet`;
    const options = {
      query: sqlQuery,
      location: "US"
    };
    const [rows] = await bigqueryClient.query(options);
    res.status(200).send(rows);
  }
  query();
});
/* ---------- Player Profile BigQuery Endpoints ---------- */
app.post("/getSignUpDate", function(req, res) {
  async function query() {
    const sqlQuery = `SELECT timestamp user_id
      FROM wildruby_pwa_prod.sign_up
      WHERE user_id = @selectedUser`;

    const options = {
      query: sqlQuery,
      location: "US",
      params: { selectedUser: req.body.params.selectedUser }
    };
    const [rows] = await bigqueryClient.query(options);
    res.status(200).send(rows);
  }
  query();
});

app.post("/getLastWager", function(req, res) {
  async function query() {
    const sqlQuery = `SELECT timestamp, cost
      FROM wildruby_pwa_prod.wager_success
      WHERE user_id = @selectedUser
      ORDER BY timestamp DESC
      LIMIT 1`;
    const options = {
      query: sqlQuery,
      location: 'US',
      params: { selectedUser: req.body.params.selectedUser }
    }
    const [rows] = await bigqueryClient.query(options);
    res.status(200).send(rows);
  }
  query();
});

app.post("/getLastGame", function(req, res) {
  async function query() {
    const sqlQuery = `SELECT game_name, bet_amount, win_amount, timestamp
      FROM wildruby_pwa_prod.game_spin
      WHERE user_id = @selectedUser
      ORDER BY timestamp DESC
      LIMIT 1`;
    const options = {
      query: sqlQuery,
      location: 'US',
      params: { selectedUser: req.body.params.selectedUser }
    }
    const [rows] = await bigqueryClient.query(options);
    res.status(200).send(rows);
  }
  query();
});

app.post("/getGames", function(req, res) {
  async function query() {
    const sqlQuery = `SELECT game_name, bet_amount, win_amount, timestamp, user_id
      FROM wildruby_pwa_prod.game_spin
      WHERE user_id = @selectedUser
      ORDER BY timestamp DESC`;
    const options = {
      query: sqlQuery,
      location: 'US',
      params: { selectedUser: req.body.params.selectedUser }
    }
    const [rows] = await bigqueryClient.query(options);
    console.log("All users games:");
    rows.forEach(row => console.log(row));
    res.status(200).send(rows);
  }
  query();
});

app.post('/getUsersWagers', function(req, res) {
  async function query() {
    const sqlQuery = `SELECT cost, timestamp, user_id
      FROM wildruby_pwa_prod.wager_success
      WHERE user_id = @selectedUser
      ORDER BY timestamp DESC`
    const options = {
      query: sqlQuery,
      location: 'US',
      params: { selectedUser: req.body.params.selectedUser }
    }
    const [rows] = await bigqueryClient.query(options)
    rows.forEach(row => console.log(row))
    res.status(200).send(rows)
  }
  query()
})

app.post('/getWagerSum', function(req, res) {
  async function query() {
    const sqlQuery = `SELECT SUM(cost)
      FROM wildruby_pwa_prod.wager_success
      WHERE user_id = @selectedUser`
    const options = {
      query: sqlQuery,
      location: 'US',
      params: { selectedUser: req.body.params.selectedUser }
    }
    const [rows] = await bigqueryClient.query(options)
    res.status(200).send(rows)
  }
  query()
})

app.post('/getAvgWagerCost', function(req, res) {
  async function query() {
    const sqlQuery = `SELECT AVG(cost)
      FROM wildruby_pwa_prod.wager_success
      WHERE user_id = @selectedUser`
    const options = {
      query: sqlQuery,
      location: 'US',
      params: { selectedUser: req.body.params.selectedUser }
    }
    const [rows] = await bigqueryClient.query(options)
    res.status(200).send(rows)
  }
  query()
})

exports.app = app;



