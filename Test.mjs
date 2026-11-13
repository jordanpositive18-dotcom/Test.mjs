import { ProxyAgent, fetch } from 'undici';

const url = 'https://www.jobsatamazon.co.uk/graphql';
let auth = process.env.AMAZON_AUTHORIZATION;
let proxyUrl = process.env.AMAZON_PROXY_URL;

if (!auth) {
  console.error('AMAZON_AUTHORIZATION is not set');
  process.exit(1);
}

if (!proxyUrl) {
  console.error('AMAZON_PROXY_URL is not set');
  process.exit(1);
}

// Clean proxy URL
proxyUrl = proxyUrl.replace(/["']/g, '').trim();
const spaceIdx = proxyUrl.indexOf(' ');
if (spaceIdx !== -1) {
  proxyUrl = proxyUrl.substring(0, spaceIdx);
}

// Ensure Authorization has Bearer prefix if it's missing
if (!auth.startsWith('Bearer ') && !auth.startsWith('Basic ')) {
  auth = `Bearer ${auth}`;
}

const dispatcher = new ProxyAgent(proxyUrl);

const res = await fetch(url, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': auth,
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Origin': 'https://www.jobsatamazon.co.uk',
    'Referer': 'https://www.jobsatamazon.co.uk/',
  },
  body: JSON.stringify({ query: '{ __typename }' }),
  dispatcher,
});

console.log('Status code with headers & proxy:', res.status);
