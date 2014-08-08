"use strict";

var fs = require('fs');
var csv = require('csv2');
var through = require('through2');
var _ = require('underscore');
var argv = require('yargs')
	.usage('Denormalize two files, joining on --join <colname>.\nUsage: $0 --join col1 path/to/file1 path/to/file2')
	.example('$0 --join --output path/to/joined_file col1 path/to/file1 path/to/file2 ', 'Join ount the lines in the given file')
	.demand('join')
	.alias('j', 'join')
	.describe('j', 'Join on col')
	.demand('output')
	.alias('o', 'output')
	.describe('o', 'Where to output')
	.demand('select')
	.alias('s', 'select')
	.describe('s', 'Columns to select for output')
	.demand(2)
	.describe('Need two files as input')
	.argv;

var readers = [];
argv._.forEach( function(path){
	var reader = read_all_csv();
	readers.push(reader);
	fs.createReadStream(path)
	.pipe(csv())
	.pipe(through({ objectMode: true },reader))
	;
});

process.on('exit', function() {
	var headers = argv.s.split(',').map(
		function(item){return item.trim();});
	console.log(headers);
	var a = readers[0].data();
	var b = readers[1].data();
	Object.keys(a).forEach(function(key){
		if(b.hasOwnProperty(key)){
			console.error(key);
			console.error(_.flatten(_.zip(a[key],b[key])));
		}
	});
});

function read_all_csv(){
	var key_col = 0;
	var is_header_line = true;
	var headers;
	var data = {};
	function pipe(chunk,enc,callback){
		if(is_header_line){
			headers = chunk.slice();
			for(key_col = 0; key_col < headers.length; key_col++){
				if(headers[key_col].toLowerCase() === argv.j.toLowerCase()){
					break;
				}
			}
			is_header_line = false;
		} 
		else {
			data[chunk[key_col]] = chunk.slice(1);
		}
		callback();
	}
	pipe.headers = function(){
		return headers;
	};
	pipe.data = function(){
		return data;
	};
	return pipe;
}
