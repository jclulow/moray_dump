#!/usr/bin/env node
/* vim: set ts=8 sts=8 sw=8 noet: */

var mod_fs = require('fs');
var mod_path = require('path');
var mod_util = require('util');
var mod_stream = require('stream');
var mod_zlib = require('zlib');
var mod_pg_dump = require('pg_dump');

var lib_moray = require('../lib/moray');
var moray_row_to_bucket_config = lib_moray.moray_row_to_bucket_config;
var moray_row_to_object = lib_moray.moray_row_to_object;

function
load_buckets_config(input_file, callback)
{
	var lbc = {
		lbc_instr: null,
		lbc_gunzip: mod_zlib.createGunzip(),
		lbc_sql: null,
		lbc_in_buckets_config: false,
		lbc_buckets: {},
		lbc_error: null,
		lbc_done: false
	};

	lbc.lbc_instr = mod_fs.createReadStream(input_file);
	lbc.lbc_sql = new mod_pg_dump.SQLTokeniser();

	lbc.lbc_sql.on('readable', function () {
		var obj;

		if (lbc.lbc_error !== null || lbc.lbc_done) {
			return;
		}

		while ((obj = lbc.lbc_sql.read()) !== null) {
			lbc_process(lbc, obj);

			if (lbc.lbc_error !== null) {
				lbc.lbc_instr.destroy();
				setImmediate(callback, lbc.lbc_error);
				return;
			}

			if (lbc.lbc_done) {
				setImmediate(callback, null, lbc.lbc_buckets);
				return;
			}
		}
	});

	lbc.lbc_instr.pipe(lbc.lbc_gunzip).pipe(lbc.lbc_sql);
}

function
lbc_process(lbc, obj)
{
	if (obj.t === 'copy_begin' && obj.table_name === 'buckets_config') {
		lbc.lbc_in_buckets_config = true;
		return;
	}

	if (!lbc.lbc_in_buckets_config) {
		return;
	}

	if (obj.t === 'copy_row') {
		var bucket = moray_row_to_bucket_config(obj.v);

		if (lbc.lbc_buckets[bucket.name]) {
			lbc.lbc_error = new Error('duplicate bucket: ' +
			    bucket.name);
			return;
		}

		lbc.lbc_buckets[bucket.name] = bucket;
		return;
	}

	if (obj.t === 'copy_end') {
		lbc.lbc_instr.destroy();
		lbc.lbc_done = true;
		return;
	}
}

function
extract_buckets_to_files(input_file, output_dir, buckets, callback)
{
	/*
	 * Create the output directory.  Fail if it exists already.
	 */
	try {
		mod_fs.mkdirSync(output_dir, parseInt('0755', 8));
	} catch (ex) {
		setImmediate(callback, ex);
		return;
	}

	var ebtf = {
		ebtf_instr: mod_fs.createReadStream(input_file),
		ebtf_gunzip: mod_zlib.createGunzip(),
		ebtf_sql: new mod_pg_dump.SQLTokeniser(),
		ebtf_buckets: buckets,
		ebtf_output_dir: output_dir,
		ebtf_done: false,
		ebtf_output: null
	};

	var finish = function (err) {
		if (ebtf.ebtf_done)
			return;
		ebtf.ebtf_done = true;

		/*
		 * Make sure we don't leak a file descriptor.
		 */
		ebtf.ebtf_instr.destroy();

		setImmediate(callback, err || null);
	};

	ebtf.ebtf_output = new BucketExtractorStream(ebtf);
	ebtf.ebtf_output.on('error', finish);
	ebtf.ebtf_output.on('finish', function () {
		finish();
	});

	ebtf.ebtf_instr.pipe(ebtf.ebtf_gunzip).pipe(ebtf.ebtf_sql).
	    pipe(ebtf.ebtf_output);
}

function
BucketExtractorStream(ebtf)
{
	var self = this;

	mod_stream.Writable.call(self, {
		objectMode: true,
		highWaterMark: 16
	});

	self.bes_ebtf = ebtf;
	self.bes_output = null;
	self.bes_bucket = null;
	self.bes_rows = 0;

	self.bes_rows_last_count = 0;
	self.bes_rows_last_stamp = Date.now();
	self.bes_interval = setInterval(function () {
		if (self.bes_bucket === null) {
			return;
		}

		var now = Date.now();
		var delta = (now - self.bes_rows_last_stamp + 1) / 1000;
		var rate = (self.bes_rows - self.bes_rows_last_count) /
		    delta;

		console.error('%s [%d] %d rows per second; bucket "%s"',
		    (new Date()).toISOString(), process.pid, rate >>> 0,
		    self.bes_bucket.name);

		self.bes_rows_last_count = self.bes_rows;
		self.bes_rows_last_stamp = now;
	}, 1000);

	self.on('finish', function () {
		clearInterval(self.bes_interval);
		if (self.bes_output !== null) {
			self.bes_output.end();
			self.bes_output = null;
		}
	});
}
mod_util.inherits(BucketExtractorStream, mod_stream.Writable);

BucketExtractorStream.prototype._write_copy_row = function
_write_copy_row(ch, done)
{
	var self = this;

	self.bes_rows++;
	var moray_obj = moray_row_to_object(self.bes_bucket, ch.v);
	var data = JSON.stringify(moray_obj) + '\n';

	if (!self.bes_output.write(data)) {
		/*
		 * When Stream#write() returns false, we ought to wait
		 * for a 'drain' event before writing more data.
		 */
		self.bes_output.once('drain', function () {
			done();
		});
		return;
	}

	done();
};

BucketExtractorStream.prototype._write_copy_end = function
_write_copy_end(ch, done)
{
	var self = this;

	self.bes_output.on('finish', function () {
		console.error(' % wrote %d rows', self.bes_rows);
		self.bes_rows = 0;
		self.bes_output = null;
		self.bes_bucket = null;
		done();
	});
	self.bes_output.end();
};

BucketExtractorStream.prototype._write_copy_begin = function
_write_copy_begin(ch, done)
{
	var self = this;

	if (ch.table_name === 'buckets_config') {
		setImmediate(done);
		return;
	}

	if (!self.bes_ebtf.ebtf_buckets[ch.table_name]) {
		console.error('WARNING: table "%s" skipped; not ' +
		    'in bucket configuration', ch.table_name);
		setImmediate(done);
		return;
	}
	self.bes_bucket = self.bes_ebtf.ebtf_buckets[ch.table_name];
	self.bes_rows_last_count = 0;
	self.bes_rows_last_stamp = Date.now();

	var path = mod_path.join(self.bes_ebtf.ebtf_output_dir,
	    ch.table_name + '.json');

	self.bes_output = mod_fs.createWriteStream(path);
	self.bes_output.on('open', function () {
		console.error(' * writing to "%s"', path);
		done();
	});
};

BucketExtractorStream.prototype._write = function
_write(ch, _, done)
{
	var self = this;

	if (self.bes_output !== null) {
		if (ch.t === 'copy_row') {
			self._write_copy_row(ch, done);
			return;
		} else if (ch.t === 'copy_end') {
			self._write_copy_end(ch, done);
			return;
		}
	} else if (ch.t === 'copy_begin') {
		self._write_copy_begin(ch, done);
		return;
	}

	setImmediate(done);
};

module.exports = {
	load_buckets_config: load_buckets_config,
	extract_buckets_to_files: extract_buckets_to_files
};
