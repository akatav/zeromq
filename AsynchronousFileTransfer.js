'use strict';

var cluster = require('cluster')
  , zmq = require('zmq')
  , fs = require('fs-extra');

var NBR_WORKERS = 3;
var NBR_FILES = 5;

function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min) + min);
}

function randomString() {
  var source = 'abcdefghijklmnopqrstuvwxyz'
    , target = [];

  for (var i = 0; i < 20; i++) {
    target.push(source[randomBetween(0, source.length)]);
  }
  return target.join('');
}

function workerTask() {

  var dealer = zmq.socket('dealer');
  dealer.identity = randomString();
  
  var access = fs.createWriteStream(dealer.identity + '.log');
  process.stdout.write = process.stderr.write = access.write.bind(access);

  console.log('I am a worker. My identity is: ' + dealer.identity);

  dealer.connect('tcp://localhost:5671');
  
  console.log(dealer.identity + ' connecting to master');

  var total = 0;
  var fileChunks = new Array();
  for(var i = 1; i <= NBR_FILES; i++) {
	  fileChunks[i] = '';
  }

  var sendMessage = function () {
	//console.log(dealer.identity + ' sending a message to master');
    dealer.send(['', 'GREETING', 'Hi Boss']);
  };
  
  var sendTaskFinishMessage = function(msg, n) {
	//console.log(dealer.identity + ' sending a task finish message to master');
	dealer.send(['', 'ACK', msg + ' ' + n]);
  };
  
  var askForChunk = function(offset, chunkSz, fileNum) {
	//console.log(dealer.identity + ' asking for chunk');
	dealer.send(['', 'ASK', offset, chunkSz, fileNum], zmq.SND_MORE);
  };

  //  Get workload from broker, until finished
  dealer.on('message', function onMessage() {
    var args = Array.apply(null, arguments);
	var n = args.length;
	
	console.log('The master has sent me: ' + dealer.identity + ' the following message' );
	for(var i=0; i < n;i++) {
		//console.log(i + '--->' + args[i]);
	}
	
	console.log('Type of message: ' + args[1].toString());

	var typeOfMessage = args[1].toString('utf8');
	if(typeOfMessage === 'ORDER') {
		console.log('I ' + dealer.identity + ' already have work assigned. '/* + args[2]*/);
	}
	else if(typeOfMessage === 'PATH') {
		var fileNum = args[3];
		console.log('The master is asking me ' + dealer.identity  + ' to read the file: ' + fileNum);
		console.log('The file the master is asking me ' + dealer.identity + 'to read is at path: ' + args[2]);
		// send message to master asking for chunks
		console.log(dealer.identity + ' - I am asking for the first chunk with offset: ' + 0 + ' in file: ' + args[2]);
		askForChunk(0, 10000, fileNum);
		//setTimeout(function() { sendTaskFinishMessage(fileNum) }, 2000);
	}
	else if(typeOfMessage === 'CHUNK') {
		
		var offset = args[2];
		var chunk = args[3];
		var fileIndex = parseFloat(args[4]);
		var chunkSz = 10000;
		console.log('The master has sent me ' +  dealer.identity + ' a chunk with offset ' + offset + ' and length: ' + chunk.toString().length + ' to write for file: ' + fileIndex);
		
		if(offset!=-1) {
			fileChunks[fileIndex]+=new Buffer(chunk, 'binary').toString();
			console.log('Written ' + fileChunks[fileIndex].length + ' of file: ' + fileIndex);
			console.log(dealer.identity + ' - I am asking for the chunk starting from offset: ' + offset + ' in file: ' + fileIndex);
			askForChunk(offset, chunkSz, fileIndex);
		}
		else {
			fileChunks[fileIndex]+=new Buffer(chunk, 'binary').toString(); //convert chunk back to binary
			console.log('Written ' + fileChunks[fileIndex].length + ' of file: ' + fileIndex);
			fs.writeFileSync('./downloads/' + fileIndex + '.zip', fileChunks[fileIndex], {'flags': 'w', 'encoding': 'binary'});
			console.log(dealer.identity + ' has completed writing file ' +  fileIndex + '.zip' + ' of size: ' + fileChunks[fileIndex].length);
			fileChunks[fileIndex] = null;
			sendTaskFinishMessage('Finished downloading and writing fileIndex: ', fileIndex);
			//sendMessage();
		}
	}
	else if(typeOfMessage === 'EOF') {
		console.log('No more work for me! '+ dealer.identity);
		dealer.removeListener('message', onMessage);
		dealer.close();
		return;
	}
	
    total++;

    setTimeout(sendMessage, 1500);
  });

  //  Tell the broker we're ready for work
  sendMessage();
}

function main() {
  var broker = zmq.socket('router');
  broker.bindSync('tcp://*:5671');

  var access = fs.createWriteStream('broker.log');
  process.stdout.write = process.stderr.write = access.write.bind(access);


  var endTime = Date.now() + 5000
    , workersFired = 0
	, fileNum = 1;
	
  var workAssigned = new Array();
  var work = [1,2,3,4,5];
  var buf = new Array();
  
  // Read all files here and store in buf 
  for(var i = 1; i <= NBR_FILES; i++) {
	  buf[i] = fs.readFileSync('./files/' + i + '.zip');
	  console.log('Length of file: ' + './files/' + i + '.zip' + ' is: ' + buf[i].length);
  }
  console.log('Read all files');

  broker.on('message', function () {
    var args = Array.apply(null, arguments)
      , identity = args[0]
      , now = Date.now();
	  
	console.log('A worker is sending me a message. It has the identity: ' + identity);
	console.log('The message had the following arguments');
	var n = args.length;
	for(var i=0;i<n;i++) {
		console.log(i + "-->" + args[i].toString());
	}
	
	var typeOfWorkerMessage = args[2].toString('utf8');
	if(typeOfWorkerMessage === 'GREETING') {
		if(fileNum <= NBR_FILES) {
			// Check if work has been assigned already to this worker
			if(workAssigned[identity]>0) {
				console.log('Worker ' + identity + ' already has file ' + workAssigned[identity] + ' to read');
				console.log('Asking worker ' + identity + ' to complete assigned work with file ' + workAssigned[identity]);
				broker.send([identity, '', 'ORDER', 'Complete assigned work!', workAssigned[identity]]);
			}
			else {
				console.log('Sending a file ' + fileNum  + ' to worker: ' + identity);
				workAssigned[identity] = fileNum;
				broker.send([identity, '', 'PATH',  './files/'+ fileNum + '.zip', fileNum]);
				fileNum++;
			}
		}
		else {
			// Either all work has been assigned or completed
			if(workAssigned[identity]>0) {
				console.log('Worker ' + identity + ' already has file ' + workAssigned[identity] + ' to read');
				console.log('Asking worker ' + identity + ' to complete assigned work with file ' + workAssigned[identity]);
				broker.send([identity, '', 'ORDER', 'Complete assigned work!', workAssigned[identity]]);
			}
			else {
				// Can close dealer
				workersFired++;
				broker.send([identity, '', 'EOF', 'EOF', -1]);
				if(workersFired==NBR_WORKERS) {
					broker.close();
					cluster.disconnect();
				}
			}
		}
	}
	else if(typeOfWorkerMessage === 'ACK') {
		// Clear workAssigned array for this dealer
		console.log(identity + ' has finished reading file');
		workAssigned[identity] = 0;
		
		if(fileNum <= NBR_FILES) {
			// send the next file to this dealer
			console.log('Sending a file ' + fileNum  + ' to worker: ' + identity);
			workAssigned[identity] = fileNum;
			broker.send([identity, '', 'PATH', './files/'+ fileNum + '.zip', fileNum]);
			fileNum++;	
		}
		else {
			// can close this dealer
			workersFired++;
			broker.send([identity, '', 'EOF', 'EOF', -1]);
			if(workersFired==NBR_WORKERS) {
				broker.close();
				cluster.disconnect();
			}
		}
	}
	else if(typeOfWorkerMessage === 'ASK') {
		// Send a chunk to this dealer
		// receiving a file chunk msg
		var offset=parseFloat(args[3]);
		var chunkSz=parseFloat(args[4]);
		var fileIndex=parseFloat(args[5]);
		
		console.log('A worker ' + identity + ' is asking me to send a chunk for file: ' + fileIndex);
		console.log('Length of the file ' + fileIndex +' is: ' + buf[fileIndex].length);
		if(offset < buf[fileIndex].length && offset+chunkSz<buf[fileIndex].length) {
			console.log('Sending a chunk from offset ' + offset + ' to offset ' + (offset+chunkSz) + ' of file ' + fileIndex + ' to worker: ' + identity );
			var chunk=buf[fileIndex].slice(offset, offset+chunkSz).toString('binary');
			console.log('Size of chunk: ' + chunk.length);
			broker.send([identity, '', 'CHUNK', offset+chunkSz, chunk.toString('binary'), fileIndex], zmq.SND_MORE);
		}
		else {
			var chunk=buf[fileIndex].slice(offset, buf[fileIndex].length).toString('binary');
			broker.send([identity, '', 'CHUNK', -1, chunk, fileIndex]);
		}
	}
  });

  for (var i=0;i<NBR_WORKERS;i++) {
	console.log('Creating worker ' + i);
    cluster.fork();
  }
}

if (cluster.isMaster) {
  console.log('Cluster in master mode');
  main();
} else  {
  console.log('Cluster in worker mode');
  workerTask();
}

