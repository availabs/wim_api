


module.exports = {
	setDatabase: function(req, res) {
		req.session.database = req.param('database');
		console.log(req.session, req.sessionID)
		res.send('ok', 200);
	},
  	_config: {}
}


