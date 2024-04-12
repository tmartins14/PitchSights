// External Imports
const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");
const helmet = require("helmet");
const cookieSession = require("cookie-session");
const passport = require("passport");
require("dotenv").config();

// Internal Imports
const keys = require("./config/keys");
const authRoutes = require("./routes/authRoutes");

// Order of import matters here ...
require("./models/User");
require("./services/passport");

mongoose.connect(keys.mongoURI);
const app = express();

app.use(helmet());

app.use(
  cookieSession({
    maxAge: 30 * 24 * 60 * 60,
    keys: [keys.cookieKey],
  })
);

// app.use(
//   cors({
//     origin: "http://localhost:3000", // Allow your client application domain
//     credentials: true, // Allows cookies to be sent
//   })
// );

app.use(cors());

app.use(passport.initialize());
app.use(passport.session());

app.use(express.json());

authRoutes(app);

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
