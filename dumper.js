#!/usr/bin/env node
/* vim: set ts=8 sts=8 sw=8 noet: */

var lib_extract = require('./lib/extract');

var INPUT_FILE = process.argv[2];
var OUTPUT_DIR = process.argv[3];
if (!INPUT_FILE || !OUTPUT_DIR) {
	console.error('ERROR: usage: dumper <input_file> ' +
	    '<output_dir>');
	process.exit(1);
}

lib_extract_moray.load_buckets_config(INPUT_FILE, function (err, buckets) {
	if (err) {
		console.error('ERROR: %s', err.stack);
		process.exit(2);
	}

	//console.log(mod_util.inspect(buckets, false, 10, true));

	lib_extract_moray.extract_buckets_to_files(INPUT_FILE,
	    OUTPUT_DIR, buckets, function (err) {
		if (err) {
			console.error('ERROR: %s', err.stack);
			process.exit(3);
		}

		console.error('ok');
	});
});
