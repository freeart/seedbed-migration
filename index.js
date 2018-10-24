const assert = require('assert'),
	async = require('async'),
	configStorage = require('etcd-fb')

module.exports = function () {
	assert(!this.migration, "field exists")

	this.migration = (config) => {
		async.autoInject({
			schema: (cb) => {
				this.orientDB.query(`
					select name, customFields.version as version from (select expand(classes) from metadata:schema)
				`).then(
					(rows) => {
						const index = {};
						for (let i = 0; i < rows.length; i++) {
							index[rows[i].name] = rows[i];
						}
						cb(null, index);
					}).catch((err) => cb(err))
			},
			migrate: (schema, cb) => {
				async.eachOfSeries(config.log, (log, timestamp, cb) => {
					async.eachSeries(log, (pair, cb) => {
						const [table, sql] = pair;
						if (!schema[table] || parseInt(timestamp, 10) > parseInt(schema[table].version, 10) || schema[table].version === null) {
							const sqls = [sql]
							if (table) {
								sqls.push(`ALTER CLASS ${table} CUSTOM version=${timestamp}`);
							}
							this.orientDB.batch(sqls, false, (err) => {
								!err && table && console.log(table, timestamp)
								cb(err)
							})
						} else {
							setImmediate(() => cb());
						}
					}, cb);
				}, cb);
			},
			directory: (migrate, cb) => {
				configStorage.set(`/${process.env.NODE_ENV}/discovery/${config.instance}/`, null, { ttl: 25 }, (err) => {
					if (err) {
						console.log(err)
					}
					cb()
				})
			},
			unlock: (directory, cb) => {
				configStorage.set(`${process.env.NODE_ENV}/discovery/${config.instance}/ready`, "1", null, cb)
			}
		}, (err) => {
			if (err) {
				return console.error(err);
			}
			console.log("done")
			async.forever(
				(next) => {
					configStorage.refresh(`${process.env.NODE_ENV}/discovery/${config.instance}/`, { ttl: 25 }, (err) => {
						if (err) {
							console.error(err);
							return setTimeout(() => process.exit(1), 1000)
						}
						setTimeout(() => next(), 5000)
					})
				}
			)
		})
	}

	return Promise.resolve();
}