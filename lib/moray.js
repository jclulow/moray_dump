#!/usr/bin/env node
/* vim: set ts=8 sts=8 sw=8 noet: */

function
moray_row_to_bucket_config(row)
{
	/*
	 * Mimic the Moray routine "loadBucket()" from "lib/objects/common.js".
	 */
	var out = {
		name: row.name,
		index: JSON.parse(row.index),
		pre: JSON.parse(row.pre),
		post: JSON.parse(row.post),
		options: JSON.parse(row.options || '{}'),
		mtime: new Date(row.mtime)
	};

	if (row.reindex_active) {
		out.reindex_active = JSON.parse(row.reindex_active);
	}

	out._indexKeys = Object.keys(out.index).map(function (k) {
		return ({
			key: k,
			lcKey: k.toLowerCase()
		});
	});

	return (out);
}

function
moray_row_to_object(bucket, row)
{
	var out = {
		bucket: bucket.name,
		key: row._key,
		value: JSON.parse(row._value),
		_id: row._id,
		_etag: row._etag,
		_mtime: parseInt(row._mtime, 10),
		_txn_snap: row._txn_snap
	};

	if (out.value.vnode !== undefined) {
		var vnodecol = null ? null : parseInt(row._vnode, 10);
		var vnodeobj = null ? null : parseInt(out.value.vnode, 10);

		if (vnodecol !== vnodeobj) {
			throw (new Error('_vnode value "' + vnodecol + '"' +
			    'did not match value.vnode value "' +
			    vnodeobj + '"'));
		}
	}

	/*
	 * Moray supports a bulk update operation which can change the value of
	 * an index column without updating the serialised JSON object in the
	 * "_value" column.  This transformation seeks to mirror the logic in
	 * "rowToObject()" in the Moray server itself.
	 */
	for (var i = 0; i < bucket._indexKeys.length; i++) {
		var prop = bucket._indexKeys[i].key;
		var colv = row[bucket._indexKeys[i].lcKey];

		/*
		 * If the index column is NULL, delete the value from the
		 * object.
		 */
		if (colv === undefined || colv === null) {
			delete (out.value[prop]);
			continue;
		}

		/*
		 * Indexes on array-valued properties are not supported.
		 */
		if (Array.isArray(out.value[prop])) {
			continue;
		}

		/*
		 * Replace the value in the object with the (potentially
		 * updated) index column value.
		 */
		out.value[prop] = colv;
	}

	return (out);
}

module.exports = {
	moray_row_to_object: moray_row_to_object,
	moray_row_to_bucket_config: moray_row_to_bucket_config
};
