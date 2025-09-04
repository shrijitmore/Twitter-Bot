const Redis = require("ioredis");

const redis = new Redis("redis://default:wDIwutvAIBPRFpCFcJRrmSKtQSirZJBF@switchyard.proxy.rlwy.net:20503");

redis.ping().then(res => {
  console.log("PING response:", res); // should log "PONG"
  redis.quit();
}).catch(err => {
  console.error("Connection failed:", err);
});
