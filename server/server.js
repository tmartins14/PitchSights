// External Imports
const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");
const helmet = require("helmet");
require("dotenv").config();

// Internal Imports
const keys = require("./config/keys");
const authRoutes = require("./routes/authRoutes");

// Order of import matters here ...
require("./models/User");
require("./services/passport");

// require("./models/User");

mongoose.connect(keys.mongoURI);
const app = express();

app.use(cors());
app.use(helmet());
app.use(express.json());

authRoutes(app);

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
