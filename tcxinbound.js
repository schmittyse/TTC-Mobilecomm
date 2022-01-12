const { delay, ServiceBusClient, ServiceBusMessage, isServiceBusError } = require("@azure/service-bus");
const axios = require('axios');
const { Connection, Statement, IN, CHAR } = require('idb-pconnector');
const config = require('./config.json');


 async function main() 
 {  
 

	let sig = await getSas();
	let connectionString = config.prod.connectUrl + `${sig}`;

	// create a Service Bus client using the connection string to the Service Bus namespace
	let sbClient = new ServiceBusClient(connectionString);

	// createReceiver() can also be used to create a receiver for a queue.
	let receiver = sbClient.createReceiver(config.prod.topicName, config.prod.subscriptionName);

	while (true) 
	{
		try
		{
			const messages = await receiver.receiveMessages(config.prod.batchCount, {maxWaitTimeInMs: config.prod.waitTime});

			messages.forEach(message => 
			{
				console.log(`Received message: ${JSON.stringify(message.body)}`);
                receiver.completeMessage(message);
                processMessage(message).catch((err) => 
				{
					console.log("Error occurred: ", err);
					process.exit(1);
				});;
			});
		} 
		catch (error) 
		{		
			console.log(`Error from source occurred: `, error);	
			if (isServiceBusError(error)) 
			{
				switch (error.code) 
				{
					case 'UnauthorizedAccess':
						if (error.message.startsWith('ExpiredToken')) 
						{
							console.log('Signature expired, reestablishing connection');
							sig = await getSas();
							let connectionString = config.prod.connectUrl + `${sig}`;
							
							// create a Service Bus client using the connection string to the Service Bus namespace
							sbClient = new ServiceBusClient(connectionString);

							// createReceiver() can also be used to create a receiver for a queue.
							receiver = sbClient.createReceiver(config.prod.topicName, config.prod.subscriptionName);
						} 
						else 
						{
							console.log(`An unrecoverable error occurred. Stopping processing. ${error.code}`, error);
						}
						break;
					case "ServiceBusy":
						// choosing an arbitrary amount of time to wait.
						console.log('Retry...')
						await delay(config.prod.retryTime);
						break;
				}
			}
		}
	}
}

async function getSas() 
{
	var options = 
	{
		headers: 
		{
      			'Content-Type': 'application/x-www-form-urlencoded',
      			 Authorization: config.prod.sasAuth
   		},
    		body: 'grant_type=client_credentials&audience=sas_token'
  	};

	try 
	{
		const res = await axios.post(config.prod.sasUrl, options.body,
		{
      			headers: options.headers,
		});

		if (!(res.data && res.data.access_token)) 
		{
			throw new Error(`Unexpected SAS response format: ${JSON.stringify(res.data)}`);
		}

		return res.data.access_token;
    
	} 
	catch (error) 
	{
	    console.log('Error getting SAS; ', error);
  	}
}

 async function processMessage(message) {
    let outstring = message.body.data;

	const statement = new Statement(connection);
	const sql = `call qsys2.SEND_DATA_QUEUE(MESSAGE_DATA => '${outstring}', DATA_QUEUE => '${config.prod.ibmIDtaq}',  DATA_QUEUE_LIBRARY => '${config.prod.ibmIDataLib}');`;
	try {

		 const results = await statement.exec(sql);

	 }
 
	 catch (err) {
		 console.error(`Error: ${err.stack}`);
	 };

  }

const connection = new Connection({ url: '*LOCAL' });

main().catch((err) => 
{
	console.log("Error occurred: ", err);
	process.exit(1);
});
