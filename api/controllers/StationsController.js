/**
 * StationsController
 *
 * @module      :: Controller
 * @description	:: A set of functions called `actions`.
 *
 *                 Actions contain code telling Sails how to respond to a certain type of request.
 *                 (i.e. do stuff, then send some JSON, show an HTML page, or redirect to another URL)
 *
 *                 You can configure the blueprint URLs which trigger these actions (`config/controllers.js`)
 *                 and/or override them with custom stationss (`config/stationss.js`)
 *
 *                 NOTE: The code you write here supports both HTTP and Socket.io automatically.
 *
 * @docs        :: http://sailsjs.org/#!documentation/controllers
 */
var googleapis = require('googleapis');
var jwt = new googleapis.auth.JWT(
		'424930963222-s59k4k5usekp20guokt0e605i06psh0d@developer.gserviceaccount.com', 
		'availwim.pem', 
		'3d161a58ac3237c1a1f24fbdf6323385213f6afc', 
		['https://www.googleapis.com/auth/bigquery']
	);
jwt.authorize();	

module.exports = {    
  	
	stationsGeo:function(req,res){

		var stationsCollection = {};
		stationsCollection.type = "FeatureCollection";
		stationsCollection.features = [];

		var sql = 'SELECT station_id,state_code,ST_AsGeoJSON(the_geom) station_location FROM tmas where num_lane_1 > 0 group by station_id,state_code,ST_AsGeoJSON(the_geom);'
		Stations.query(sql,{},function(err,data){
			if (err) {res.send('{status:"error",message:"'+err+'"}',500);return console.log(err);}
				data.rows.forEach(function(stations){
					var stationsFeature = {};
					stationsFeature.type="Feature";
					stationsFeature.geometry = JSON.parse(stations.station_location);
					stationsFeature.properties = {};
					stationsFeature.properties.station_id = stations.station_id;
					stationsFeature.properties.state_code = stations.state_code;
					stationsCollection.features.push(stationsFeature);

					});

			res.send(stationsCollection);
		});
 	},
	ClassStationsGeo:function(req,res){

		var stationsCollection = {};
		stationsCollection.type = "FeatureCollection";
		stationsCollection.features = [];

		var sql = 'SELECT  station_id,state_code,ST_AsGeoJSON(the_geom) station_location FROM tmas where num_lanes1 > 0 group by station_id,state_code,ST_AsGeoJSON(the_geom);'
		Stations.query(sql,{},function(err,data){
			if (err) {res.send('{status:"error",message:"'+err+'"}',500);return console.log(err);}
				data.rows.forEach(function(stations){
					var stationsFeature = {};
					stationsFeature.type="Feature";
					stationsFeature.geometry = JSON.parse(stations.station_location);
					stationsFeature.properties = {};
					stationsFeature.properties.station_id = stations.station_id;
					stationsFeature.properties.state_code = stations.state_code;
					stationsCollection.features.push(stationsFeature);

					});

			res.send(stationsCollection);
		});
 	},
 	getAllStations:function(req,res){
 		var database = req.param('database');
 		googleapis.discover('bigquery', 'v2').execute(function(err, client) {
		    //jwt.authorize(function(err, result) {
		    	if (err) console.log(err);
			    var request = client.bigquery.jobs.query({
			    	kind: "bigquery#queryRequest",
			    	projectId: 'avail-wim',
			    	timeoutMs: '30000'
			    });
			    request.body = {};
			    request.body.query = 'select state_fips,station_id,count(1) as num_trucks FROM [tmasWIM12.'+database+'] where  state_fips is not null group by state_fips,station_id order by state_fips,num_trucks desc;';
			    request.body.projectId = 'avail-wim';
			    //console.log(request);
		      	request
	        	.withAuthClient(jwt)
	        	.execute(function(err, response) {
	          		if (err) console.log(err);
	          		//console.log(response);
	          		res.json(response);
	        	});
		    //});
		});
 	},

 	getStateStations:function(req,res){
 		var state_fips = req.param('stateFips'),
 			database = req.param('database');
 		googleapis.discover('bigquery', 'v2').execute(function(err, client) {
		    //jwt.authorize(function(err, result) {
		    	if (err) console.log(err);
			    var request = client.bigquery.jobs.query({
			    	kind: "bigquery#queryRequest",
			    	projectId: 'avail-wim',
			    	timeoutMs: '30000'
			    });
			    request.body = {};
			    request.body.query = 'select station_id, year,count( distinct num_months) as numMon,count(distinct num_days) as numDay, count(distinct num_hours)/8760 as percent, sum(total)/count(distinct num_days) as AADT from (select  station_id,year,concat(string(year),string(month)) as num_months,concat(string(year),string(month),string(day)) as num_days ,concat(string(year),string(month),string(day),string(hour)) as num_hours, count(station_id) as total FROM [tmasWIM12.'+database+'] where state_fips="'+state_fips+'" and state_fips is not null group by station_id,year,num_hours,num_months,num_days) group by station_id,year order by station_id,year';
			    request.body.projectId = 'avail-wim';
			    //console.log(request);
		      	request
	        	.withAuthClient(jwt)
	        	.execute(function(err, response) {
	          		if (err) console.log(err);
	          		//console.log(response);
	          		res.json(response);
	        	});
		    //});
		});
 	},
 	getStationGeoForState: function(req, res) {
 		if(typeof req.param('statefips') == 'undefined'){
 			res.send('{status:"error",message:"state FIPS required"}',500);
 			return;
 		}
 		var stateFIPS = +req.param('statefips');

 		googleapis.discover('bigquery', 'v2').execute(function(err, client) {
	    	if (err) {
	    		console.log(err);
	    		return;
	    	}
 		
			var featureCollection = {
				type: "FeatureCollection",
				features: []
			}

			var sql = "SELECT station_id,func_class_code,method_of_vehicle_class,"+
				"method_of_truck_weighing,type_of_sensor,latitude,longitude "+
				"FROM [tmasWIM12.allStations] "+
				"WHERE NOT latitude = '        ' AND NOT latitude = '       0'"+
				"AND state_fips = '" +stateFIPS+ "' "+
				"GROUP BY station_id,func_class_code,method_of_vehicle_class,"+
				"method_of_truck_weighing,type_of_sensor,latitude,longitude;";

		    var request = client.bigquery.jobs.query({
		    	kind: "bigquery#queryRequest",
		    	projectId: 'avail-wim',
		    	timeoutMs: '30000'
		    });
		    request.body = {};
		    request.body.query = sql;
		    request.body.projectId = 'avail-wim';
	      	request.withAuthClient(jwt)
	        	.execute(function(err, BQobj) {
			    	if (err) {
			    		console.log(err);
			    		return;
			    	}
			    	var schema = [];
			    	BQobj.schema.fields.forEach(function(d) {
			    		schema.push(d.name);
			    	})

			    	BQobj.rows.forEach(function(d) {
			    		var feature = {
			    			type:'Feature',
			    			geometry: {
			    				type:'Point',
			    				coordinates: [0, 0]
			    			},
			    			properties: {}
			    		};
			    		schema.forEach(function(name, i) {
			    			if (name != 'latitude' && name != 'longitude') {
				    			feature.properties[name] = d.f[i].v;
				    		} else if (name == 'longitude') {
				    			var lng = d.f[i].v.replace(/^ (\d\d)/, '-$1.');
				    			lng = lng.replace(/^(\d\d\d)/, '-$1.');
				    			feature.geometry.coordinates[0] = lng;
				    		} else if (name == 'latitude') {
				    			var lat = d.f[i].v.replace(/^ ?(\d\d)/, '$1.');
				    			feature.geometry.coordinates[1] = lat;
				    		}
			    		})
			    		featureCollection.features.push(feature);
			    	})

	          		res.json(featureCollection);
	        	});
		});

		// Stations.query(sql, {}, function(err, data){
		// 	if (err) {
		// 		res.send('{status:"error",message:"'+err+'"}',500);
		// 		return console.log(err);
		// 	}

		// 	data.rows.forEach(function(stations){
		// 		var stationsFeature = {};
		// 		stationsFeature.type="Feature";
		// 		stationsFeature.geometry = JSON.parse(stations.geom);
		// 		stationsFeature.properties = {};
		// 		stationsFeature.properties.station_id = stations.station_id;
		// 		stationsCollection.features.push(stationsFeature);

		// 		});

		// 	res.send(stationsCollection);
		// });
 	},
 	getStationData:function(req,res){
 		if(typeof req.param('station_id') == 'undefined'){
 			res.send('{status:"error",message:"station_id required"}',500);
 			return;
 		}
 		var station_id = req.param('station_id'),
 			depth = req.param('depth'),
 			database = req.param('database');


 		var select = {
 			1: 'year',
 			2: 'month',
 			3: 'day',
 			4: 'hour'
 		};

 		var SQL = generateSQL();

 		googleapis.discover('bigquery', 'v2').execute(function(err, client) {
		    	if (err) {
		    		console.log(err);
		    		return;
		    	}

			    var request = client.bigquery.jobs.query({
			    	kind: "bigquery#queryRequest",
			    	projectId: 'avail-wim',
			    	timeoutMs: '30000'
			    });

			    request.body = {};
			    request.body.query = SQL;
			    request.body.projectId = 'avail-wim';
		      	request.withAuthClient(jwt)
	        	.execute(function(err, response) {
	          		if (err) {
	          			console.log(err);
	          			return;
	          		}
	          		res.json(response);
	        	});
		});
 		function generateSQL() {
 			var sql	= "SELECT " + select[depth.length] + ", class, total_weight as weight, count(*) as amount "
 				+ "FROM [tmasWIM12."+database+"] "
 				+ "WHERE station_id = '"+station_id+"' "
 				+ addPredicates()
 				+ "GROUP BY " + select[depth.length] + ", class, weight "
 				+ "ORDER BY " + select[depth.length] + ";";
 			return sql;
 		}
 		function addPredicates() {
 			var preds = '';
 			for (var i = 1; i < depth.length; i++) {
 				preds += 'AND ' + select[i] + ' = ' + depth[i] + ' ';
 			}
 			return preds;
 		}
	},
 	getTrucks:function(req,res){
 		var station_id = req.param('stationId'),
 			database = req.param('database');
 		//console.time('auth');
 		googleapis.discover('bigquery', 'v2').execute(function(err, client) {
		    //jwt.authorize(function(err, result) {
		    	if (err) console.log(err);
			    var request = client.bigquery.jobs.query({
			    	kind: "bigquery#queryRequest",
			    	projectId: 'avail-wim',
			    	timeoutMs: '30000'
			    });
			    //console.timeEnd('auth');
			    request.body = {};
			    request.body.query = 'select num_days,count(num_days) as numDay,month,day,class,year from(select station_id,class,concat(string(year),string(month),string(day)) as num_days, month,day,year FROM [tmasWIM12.'+database+'] where station_id="'+station_id+'" and station_id is not null) group by num_days,month,day,class,year';
			    request.body.projectId = 'avail-wim';
			    //console.log(request);
			    //console.time('query');
		      	request.withAuthClient(jwt)
	        	.execute(function(err, response) {
	          		if (err) console.log(err);
	          		//console.log(response);
	          		//console.timeEnd('query');
	          		//console.time('send');
	          		res.json(response);
	          		//console.timeEnd('send');
	        	});
		    //});
		});
 	},
 	getYears:function(req,res){
 		var database = req.param('database');
 		googleapis.discover('bigquery', 'v2').execute(function(err, client) {
		    jwt.authorize(function(err, result) {
		    	if (err) console.log(err);
		    	//console.log()
			    var request = client.bigquery.jobs.query({
			    	kind: "bigquery#queryRequest",
			    	projectId: 'avail-wim',
			    	timeoutMs: '30000'
			    });
			    request.body = {};
			    request.body.query = 'Select min(year),max(year) from [tmasWIM12.'+database+']';
			    request.body.projectId = 'avail-wim';
			    //console.log(request);
		      	request.withAuthClient(jwt)
	        	.execute(function(err, response) {
	          		if (err) console.log(err);
	          		//console.log(response);
	          		res.json(response);
	        	});
		    });
		});
 	},


  /**
   * Overrides for the settings in `config/controllers.js`
   * (specific to StationsController)
   */
  _config: {}

  
};
