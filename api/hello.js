// api/hello.js (CommonJS)
module.exports = async function handler(req, res) {
  res.status(200).json({ ok: true, now: new Date().toISOString() });
};
