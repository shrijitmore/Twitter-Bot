const { TwitterApi } = require('twitter-api-v2');
const dotenv = require('dotenv');
dotenv.config();
const client = new TwitterApi({
  appKey: process.env.TWITTER_API_KEY,
  appSecret: process.env.TWITTER_API_SECRET,
  accessToken: process.env.TWITTER_ACCESS_TOKEN,
  accessSecret: process.env.TWITTER_ACCESS_SECRET,
});

(async () => {
  try {
    const me = await client.v2.me();
    console.log('Connected as', me.data.username);
  } catch (e) {
    console.error('Auth failed:', e);
  }
})();
